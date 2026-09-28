/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-09-28 21:55:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-09-28 21:55:00
 * @FilePath: \go-wsc\hub\dead_node_probe_test.go
 * @Description: 死节点探测异步化回归测试
 *
 * 覆盖语义（miniredis 真实链路）：
 *   - PUBLISH 返回 0 + 节点心跳正常 → 走异步探测，不计入 deadNodes
 *   - 同步路径不阻塞：publishToTargetedNodes 耗时远小于重连窗口（修复前内联 sleep 300ms）
 *   - 探测 goroutine 在重连窗口后重试发布，订阅恢复则消息送达
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */

package hub

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kamalyes/go-cachex"
	wscconfig "github.com/kamalyes/go-config/pkg/wsc"
	"github.com/kamalyes/go-wsc/cluster"
	"github.com/kamalyes/go-wsc/models"
)

// newProbeTestHub 构造接入 miniredis 的最小 Hub：
// pubsub 指向 miniredis（真实 PUBLISH 语义），nodeRegistry 登记存活节点
func newProbeTestHub(t *testing.T) (*Hub, *miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	hub := NewHub(wscconfig.Default())
	hub.nodeID = "node-a"
	hub.pubsub = cachex.NewPubSub(redisClient)
	hub.nodeRegistry = cluster.NewNodeRegistry(nil, "node-a", "", "wsc:nodes:grpc", "wsc:nodes:heartbeat", nil)
	// 注册即存活（心跳新鲜代理）：GetNodeAddr 命中 → 疑似死节点走异步探测
	hub.nodeRegistry.SetNodeAddr("node-b", "127.0.0.1:1")
	return hub, mr, redisClient
}

// TestPublishToTargetedNodesProbeNotBlocking 死节点探测异步化：
// 频道无人订阅（PUBLISH=0）且心跳正常时，同步调用立即返回不内联等待，
// 探测 goroutine 在重连窗口后重试发布，消息送达恢复的订阅
func TestPublishToTargetedNodesProbeNotBlocking(t *testing.T) {
	hub, _, redisClient := newProbeTestHub(t)
	defer func() {
		_ = hub.pubsub.Close()
		_ = redisClient.Close()
	}()

	channel := hub.pubsub.ResolveChannel(
		hub.config.RedisRepository.PubSub.GetNodeChannelPrefix() + "node-b")

	// 非 SendMessage 控制消息：探测重试仍无人订阅时只记 warn，不触发 P2P 兜底
	dispatch := &models.DistributedMessage{Type: models.OperationTypeClientReclaim, NodeID: "node-b"}

	start := time.Now()
	deadNodes, err := hub.publishToTargetedNodes(context.Background(), dispatch, []string{"node-b"})
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.Empty(t, deadNodes, "心跳正常的疑似死节点走异步探测，不计入 deadNodes")
	assert.Less(t, elapsed, deadNodeProbeRetryDelay, "同步路径不得阻塞等待重连窗口（修复前此处内联 sleep 300ms）")

	// 首次 PUBLISH 时无人订阅（返回 0 触发探测）；返回后模拟订阅恢复，
	// 探测重试（+300ms）应把消息投给本订阅
	sub := redisClient.Subscribe(context.Background(), channel)
	defer func() { _ = sub.Close() }()
	_, subErr := sub.ReceiveTimeout(context.Background(), time.Second)
	require.NoError(t, subErr, "订阅应建立成功")

	select {
	case msg := <-sub.Channel():
		assert.NotEmpty(t, msg.Payload, "探测重试应把消息投递给恢复的订阅")
	case <-time.After(2 * time.Second):
		t.Fatal("探测 goroutine 未在重连窗口后重试发布")
	}
}
