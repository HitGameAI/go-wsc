/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-09-23 19:08:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-09-23 19:08:00
 * @FilePath: \go-wsc\connection\record.go
 * @Description: 连接域连接记录管理器 —— 记录构造 / 异步落库 / 停机批量终态
 *
 * 从 hub/registry.go 域化下沉：连接记录是连接的持久化影子（何时来、何时走、
 * 从哪来），构造与落库语义归连接域；仓储经 RecordHost 端口动态读取，
 * 支持编排层运行期注入（构造时未注入则各方法 no-op 降级）。
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */

package connection

import (
	"context"
	"time"

	"github.com/kamalyes/go-sqlbuilder"
	"github.com/kamalyes/go-toolbox/pkg/syncx"

	"github.com/kamalyes/go-wsc/models"
	"github.com/kamalyes/go-wsc/spi"
)

// RecordManager 连接记录管理器（连接域）
//
// 覆盖连接记录的构造（Client → ConnectionRecord 快照）与终态写路径
// （注册异步保存 / 注销异步标记断开 / 停机批量终态）
type RecordManager struct {
	host   RecordHost
	logger spi.Logger
}

// NewRecordManager 构造连接记录管理器
func NewRecordManager(host RecordHost) *RecordManager {
	logger := host.GetLogger()
	if logger == nil {
		logger = spi.NewDefaultLogger()
	}
	return &RecordManager{host: host, logger: logger}
}

// store 返回连接记录仓储（未注入时 nil，调用方 no-op 降级）
func (m *RecordManager) store() spi.ConnectionStore {
	return m.host.GetConnectionRecordRepo()
}

// Create 构造连接记录（内存对象，供异步保存 + 连接回调使用）
func (m *RecordManager) Create(client *models.Client) *models.ConnectionRecord {
	record := &models.ConnectionRecord{
		ConnectionID: client.ID,
		UserID:       client.UserID,
		AppID:        client.GetAppID(),
		Namespace:    client.GetNamespace(),
		NodeID:       client.NodeID,
		NodeIP:       client.NodeIP,
		NodePort:     client.NodePort,
		ClientIP:     client.GetClientIP(),
		Protocol:     client.ConnectionType,
		ClientType:   client.ClientType,
		ConnectedAt:  client.ConnectedAt,
		IsActive:     true,
	}

	// 设置 metadata（线程安全读取快照）
	record.Metadata = sqlbuilder.MapAny(client.GetMetadataSnapshot())

	return record
}

// Save 保存或更新连接记录到数据库（仓储未注入时 no-op）
// ctx 应为 client.Context（带 client 维度的 trace_id），实现异步保存的全链路追踪
func (m *RecordManager) Save(ctx context.Context, record *models.ConnectionRecord) {
	if m.store() == nil {
		return
	}
	syncx.Go(ctx).
		WithTimeout(10 * time.Second).
		OnError(func(err error) {
			m.logger.WarnContextKV(ctx, "保存连接记录失败",
				"connection_id", record.ConnectionID,
				"error", err,
			)
		}).
		ExecWithContext(func(ctx context.Context) error {
			return m.store().Upsert(ctx, record)
		})
}

// disconnectionShutdownChunkSize 停机批量直调的单块上限
// 每条 entry 展开 11 个 SQL 参数（5 列 CASE WHEN 各 2 + IN 1），500 条即 5.5k
// 参数，控制在三方言占位符上限内（PG 系 65535，MySQL max_allowed_packet 语境下亦安全）
const disconnectionShutdownChunkSize = 500

// MarkDisconnected 标记连接为已断开（攒批路径，batcher 未注入时 no-op）
// 构造 DisconnectionEntry 快照提交到 DisconnectionBatcher，由后台攒批合并为
// CASE WHEN 单 SQL 落库；快照在断连瞬间冻结 DisconnectedAt/ConnectedAt，
// flush 延迟不虚增 duration；队列满丢弃与原记录池可丢弃语义一致
func (m *RecordManager) MarkDisconnected(client *models.Client) {
	batcher := m.host.GetDisconnectionBatcher()
	if batcher == nil {
		return
	}
	if !batcher.Submit(&models.DisconnectionEntry{
		ConnectionID:   client.ID,
		ConnectedAt:    client.ConnectedAt,
		DisconnectedAt: time.Now(),
		Reason:         models.DisconnectReasonClientRequest,
	}) {
		m.logger.DebugKV("断连终态提交丢弃（攒批队列满）",
			"connection_id", client.ID,
		)
	}
}

// MarkDisconnectedBatch 停机批量标记连接断开（仓储未注入时 no-op）
// 与单连接路径的差异：reason 为 ServerShutdown + 关闭码 1001（客户端据此识别
// 服务端主动离开并重连）；不走攒批队列而是分块同步直调 BatchMarkDisconnected——
// 停机路径需在本方法返回后立即被 FinalizeOnDisconnect 读 duration 算终评，保序优先
func (m *RecordManager) MarkDisconnectedBatch(clients []*models.Client) {
	if m.store() == nil || len(clients) == 0 {
		return
	}
	now := time.Now()
	entries := make([]*models.DisconnectionEntry, len(clients))
	for i, client := range clients {
		entries[i] = &models.DisconnectionEntry{
			ConnectionID:   client.ID,
			ConnectedAt:    client.ConnectedAt,
			DisconnectedAt: now,
			Reason:         models.DisconnectReasonServerShutdown,
			Code:           1001,
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for start := 0; start < len(entries); start += disconnectionShutdownChunkSize {
		end := min(start+disconnectionShutdownChunkSize, len(entries))
		if err := m.store().BatchMarkDisconnected(ctx, entries[start:end]); err != nil {
			m.logger.WarnContextKV(ctx, "shutdown: 批量标记连接断开失败",
				"chunk_size", end-start,
				"error", err,
			)
		}
	}
}
