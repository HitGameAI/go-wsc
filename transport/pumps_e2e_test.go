/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-09-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-09-28 00:00:00
 * @FilePath: \go-wsc\transport\pumps_e2e_test.go
 * @Description: 读写泵端到端测试 —— 真 Hub + 真握手 + 真连接的下行链路锁定
 *
 * 背景：域化重构期间泵启动接线丢失（读写泵私有方法全库零调用），所有连接
 * 只进不出——升级/注册/在线状态全部正常，但 pong 与业务消息永远滞留通道
 * 本测试从外部行为锁定"泵真正启动"这一接线，任何一环断裂即可在此复现
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */

package transport

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/kamalyes/go-toolbox/pkg/json"
	"github.com/kamalyes/go-wsc/constants"
	wschub "github.com/kamalyes/go-wsc/hub"
	"github.com/kamalyes/go-wsc/models"
	"github.com/kamalyes/go-wsc/routing"
	"github.com/kamalyes/go-wsc/spi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClientPumpsEndToEnd 真 Hub 端到端全链路：
// 升级 → 注册（注册表可查 + 四件套初始化）→ 注册确认经写泵到达 →
// 应用层 ping → pong（读泵消费 + 控制通道 + 写泵写出）→
// 协议级 PING → pong 控制帧（SetPingHandler + PongCh + 写泵）→
// 客户端断连 → 读泵退出 → 异步注销 → 注册表归零
func TestClientPumpsEndToEnd(t *testing.T) {
	cfg := newUpgraderTestConfig()
	cfg.ResponseHeaders.SendRegisteredMessage = true

	h := wschub.NewHub(cfg)
	require.NotNil(t, h)
	go h.Run()
	defer func() { _ = h.SafeShutdown() }()

	u := NewUpgrader(cfg, h).
		WithNodeID("node-e2e").
		WithLogger(h.GetLogger()).
		WithHubContext(h.Context())

	srv := httptest.NewServer(http.HandlerFunc(u.HandleWebSocketUpgrade))
	defer srv.Close()

	dialURL := strings.Replace(srv.URL, "http://", "ws://", 1) +
		"?user_id=u-e2e&user_type=customer&device_id=d-e2e&app_id=app-e2e&namespace=ns-e2e"
	conn, _, err := websocket.DefaultDialer.Dial(dialURL, nil)
	require.NoError(t, err)

	// 协议级 pong 控制帧由 ReadMessage 循环内经 SetPongHandler 分发
	pongCh := make(chan string, 1)
	conn.SetPongHandler(func(appData string) error {
		pongCh <- appData
		return nil
	})

	// 注册完成：注册表可查（异步 handleRegister）
	require.Eventually(t, func() bool {
		return h.GetShardedRegistry().GetClientCount() == 1
	}, 3*time.Second, 10*time.Millisecond, "升级后应完成注册")

	// 读文本消息并解析 message_type（下行链路断言的统一入口）
	readMessageType := func() string {
		mt, data, rerr := conn.ReadMessage()
		require.NoError(t, rerr)
		require.Equal(t, websocket.TextMessage, mt)
		var m struct {
			MessageType models.MessageType `json:"message_type"`
		}
		require.NoError(t, json.Unmarshal(data, &m))
		return string(m.MessageType)
	}

	// 1. 注册确认消息经写泵到达：SendRegisteredMessage 在 Register 返回后立即调用，
	//    依赖注册入口同步初始化的 SendChan 缓冲，泵启动后排空写出
	assert.Equal(t, string(models.MessageTypeClientRegistered), readMessageType(),
		"注册确认应经写泵到达客户端")

	// 2. 应用层 ping → pong：读泵消费文本 → 心跳保活 → 控制通道 → 写泵写出
	pingPayload, merr := json.Marshal(map[string]string{
		"message_type": string(models.MessageTypePing),
		"content":      "hb-e2e",
	})
	require.NoError(t, merr)
	require.NoError(t, conn.WriteMessage(websocket.TextMessage, pingPayload))
	assert.Equal(t, string(models.MessageTypePong), readMessageType(),
		"应用层 ping 应经读写泵收到 pong")

	// 3. 协议级 PING → pong 控制帧：SetPingHandler 保活 + PongCh 投递 + 写泵 WriteControl
	// 客户端侧需保持 ReadMessage 循环驱动控制帧分发（文本消息断言已完成，起 drain 循环）
	go func() {
		for {
			if _, _, rerr := conn.ReadMessage(); rerr != nil {
				return
			}
		}
	}()
	require.NoError(t, conn.WriteControl(websocket.PingMessage, []byte("ka-e2e"), time.Now().Add(3*time.Second)))
	select {
	case got := <-pongCh:
		assert.Equal(t, "ka-e2e", got, "协议级 PING 应收到回显 pong 控制帧")
	case <-time.After(3 * time.Second):
		t.Fatal("协议级 PING 未收到 pong 控制帧（SetPingHandler/PongCh/写泵链路断裂）")
	}

	// 4. 客户端优雅断连 → 读泵 ReadMessage 报错退出 → defer Unregister → 注册表归零
	require.NoError(t, conn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, "bye"),
		time.Now().Add(3*time.Second)))
	require.NoError(t, conn.Close())
	require.Eventually(t, func() bool {
		return h.GetShardedRegistry().GetClientCount() == 0
	}, 3*time.Second, 10*time.Millisecond, "断连后读泵退出并触发异步注销")
}

// e2eGroupStore 群组仓储最小 fake（嵌入接口，仅实现入组/投递链路实际调用的方法；
// key 维度按 (app, ns, gid) 全维度——若入组写入与投递查询的 ns 传参不一致会在此暴露维度错配）
type e2eGroupStore struct {
	spi.GroupStore
	mu      sync.Mutex
	members map[string]map[string]struct{}
}

func (s *e2eGroupStore) key(appID, ns, gid string) string {
	return appID + ":" + ns + ":" + gid
}

// GetGroup 未创建的组返回 ErrGroupNotFound，驱动入组侧自动创建分支（与真实仓储契约一致）
func (s *e2eGroupStore) GetGroup(_ context.Context, _, _, _ string) (*models.Group, error) {
	return nil, models.ErrGroupNotFound
}

// CreateGroup 自动创建业务组（入组侧 GetGroup NotFound 后调用）
func (s *e2eGroupStore) CreateGroup(_ context.Context, _ *models.Group) error { return nil }

func (s *e2eGroupStore) AddMembers(_ context.Context, appID, ns, gid string, userIDs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := s.key(appID, ns, gid)
	if s.members[k] == nil {
		s.members[k] = make(map[string]struct{})
	}
	for _, u := range userIDs {
		s.members[k][u] = struct{}{}
	}
	return nil
}

func (s *e2eGroupStore) RemoveMembers(_ context.Context, appID, ns, gid string, userIDs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := s.key(appID, ns, gid)
	for _, u := range userIDs {
		delete(s.members[k], u)
	}
	return nil
}

func (s *e2eGroupStore) GetMembers(_ context.Context, appID, ns, gid string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for u := range s.members[s.key(appID, ns, gid)] {
		out = append(out, u)
	}
	return out, nil
}

func (s *e2eGroupStore) IsMember(_ context.Context, appID, ns, gid, userID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.members[s.key(appID, ns, gid)][userID]
	return ok, nil
}

func (s *e2eGroupStore) GetMemberCount(_ context.Context, appID, ns, gid string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(len(s.members[s.key(appID, ns, gid)])), nil
}

// GetMultiGroupMembers 批量获取多群组成员（群组可靠投递的批量预取入口，无 ns 维度跨租户查）
// 本 fake 成员仅按 (app, ns, gid) 写入，此处按 ns 遍历匹配（测试 ns 唯一，生产由 Redis app 桶实现）
func (s *e2eGroupStore) GetMultiGroupMembers(ctx context.Context, appID string, groupIDs []string) (map[string][]string, error) {
	s.mu.Lock()
	nsList := make([]string, 0, len(s.members))
	for k := range s.members {
		parts := strings.SplitN(k, ":", 3)
		if len(parts) == 3 && parts[0] == appID {
			nsList = append(nsList, parts[1])
		}
	}
	s.mu.Unlock()

	out := make(map[string][]string, len(groupIDs))
	for _, gid := range groupIDs {
		seen := make(map[string]struct{})
		for _, ns := range nsList {
			members, err := s.GetMembers(ctx, appID, ns, gid)
			if err != nil {
				return nil, err
			}
			for _, m := range members {
				seen[m] = struct{}{}
			}
		}
		for m := range seen {
			out[gid] = append(out[gid], m)
		}
	}
	return out, nil
}

// SetInvalidateNotifier SetGroupRepository 注入的拓扑失效回调，裸仓储无缓存 no-op
func (s *e2eGroupStore) SetInvalidateNotifier(func(string, string)) {}

// TestGroupAutoJoinAndDeliveryEndToEnd 群组链路端到端：
// 连接注册（带业务组）→ JoinMemberGroupOnConnect 自动入组 → 群组可靠投递送达 SendChan。
// 背景：与读写泵丢失同款——域化重构期间注册路径的自动入组接线丢失
// （JoinMemberGroupOnConnect 全库仅测试调用），群组桶永远无成员，投递报"群组没有成员"
func TestGroupAutoJoinAndDeliveryEndToEnd(t *testing.T) {
	cfg := newUpgraderTestConfig()
	h := wschub.NewHub(cfg)
	require.NotNil(t, h)
	gs := &e2eGroupStore{members: make(map[string]map[string]struct{})}
	h.SetGroupRepository(gs)
	go h.Run()
	defer func() { _ = h.SafeShutdown() }()

	// 直接构造带业务组的客户端走注册全链（RegisterSync 同步跑 handleRegister：
	// 四件套初始化 → AddClient → 自动入组 → 泵启动（Conn=nil 短路））
	client := &models.Client{
		ID:        "c-grp-e2e",
		UserID:    "u-grp-e2e",
		UserType:  models.UserTypeCustomer,
		Namespace: "ns-e2e",
		GroupIDs:  []string{"biz-grp-e2e"},
	}
	h.RegisterSync(client)

	// 注册后自动入组完成（GroupStore 可查成员）
	require.Eventually(t, func() bool {
		ok, _ := gs.IsMember(context.Background(),
			constants.NormalizeAppID(""), "ns-e2e", "biz-grp-e2e", "u-grp-e2e")
		return ok
	}, 3*time.Second, 10*time.Millisecond, "注册时应自动加入业务组")

	// 群组可靠投递（与 notify sendGroup 同构：ns + groupIDs + RequireAck）
	msg := models.NewHubMessage().
		SetMessageType(models.MessageTypeNotice).
		SetSender(models.UserTypeSystem.String()).
		SetSenderType(models.UserTypeSystem).
		SetContent("group-e2e").
		SetMessageID("m-grp-e2e").
		SetRequireAck(true)
	ctx := routing.NewRoute().
		WithNamespace("ns-e2e").
		WithGroupIDs([]string{"biz-grp-e2e"}).
		Inject(context.Background())
	result := h.Deliver(ctx, msg, false)
	require.NotNil(t, result)
	assert.GreaterOrEqual(t, result.Sent+result.StoredOffline, 1, "群组投递应命中成员")

	// 成员本地在线 → 消息经 SendChan 到达（泵未启动，读缓冲验证投递本身）
	select {
	case data := <-client.SendChan:
		var m struct {
			MessageType models.MessageType `json:"message_type"`
		}
		require.NoError(t, json.Unmarshal(data, &m))
		assert.Equal(t, string(models.MessageTypeNotice), string(m.MessageType))
	case <-time.After(3 * time.Second):
		t.Fatal("群组消息未到达成员 SendChan")
	}
}
