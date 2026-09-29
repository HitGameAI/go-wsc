/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-09-30 09:21:17
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-09-30 09:21:17
 * @FilePath: \go-wsc\batcher\disconnection_batcher.go
 * @Description: 断连终态批量更新器
 *   基于 syncx.BatchProcessor 泛型批量处理器实现
 *   正常注销路径的断连终态（duration/disconnected_at/is_abnormal）攒批落库，
 *   替代逐条 UPDATE 直打 DB（断连风暴下 26w 断连 = 26w 条独立 UPDATE + 26w 个
 *   10s 超时 goroutine，是 DB 写入洪峰与 goroutine 爆炸的主因）
 *
 * 工作流程：
 *   1. 注销路径调用 Submit（非阻塞，快照断连瞬间的终态字段）
 *   2. BatchProcessor 后台 worker 收集，满 batchSize 或每 flushInterval 触发 flush
 *   3. flush 透传给 connect 仓储的 BatchMarkDisconnected
 *      （CASE WHEN 单 SQL 合并，批内去重由仓储层统一处理，此处不重复实现）
 *   4. 停机时在 StopRecords 中 Stop，flush 剩余数据后退出
 *      （连接清理的 unregister 风暴发生在 StopTracking 之后，须待本段才停）
 *
 * 参数为包级常量而非配置项：断连是连接生命周期的确定性终点事件，
 * 队列 8192 / 批 200 / 秒级 flush 覆盖 26w 断连/分钟的突发（667/s 稳态，
 * 单 SQL flush 耗时 50ms 量级即 2000/s 吞吐，余量充足）
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */

package batcher

import (
	"context"
	"time"

	"github.com/kamalyes/go-toolbox/pkg/syncx"
	"github.com/kamalyes/go-wsc/models"
)

const (
	// disconnectionQueueSize 提交队列容量（覆盖一个 flush 周期内的断连突发）
	disconnectionQueueSize = 8192
	// disconnectionBatchSize 单批条目数（与消息状态/心跳攒批同量级）
	disconnectionBatchSize = 200
	// disconnectionFlushInterval 低流量兜底 flush 间隔
	disconnectionFlushInterval = time.Second
)

// DisconnectionBatcher 断连终态批量更新器
type DisconnectionBatcher struct {
	hub       StorageBatchWriter
	processor *syncx.BatchProcessor[*models.DisconnectionEntry]
}

// NewDisconnectionBatcher 创建断连终态批量更新器
func NewDisconnectionBatcher(hub StorageBatchWriter) *DisconnectionBatcher {
	b := &DisconnectionBatcher{hub: hub}
	b.processor = syncx.NewBatchProcessor(disconnectionQueueSize, disconnectionBatchSize, disconnectionFlushInterval, b.flush)
	return b
}

// Submit 非阻塞提交断连终态条目
// 队列满时返回 false（上层记录池本就是可丢弃语义，风暴极端场景下
// 丢弃终态只影响审计统计精度，不影响连接清理与消息投递主流程）
func (b *DisconnectionBatcher) Submit(entry *models.DisconnectionEntry) bool {
	return b.processor.Submit(entry)
}

// Stop 停止更新器，flush 剩余数据后退出
func (b *DisconnectionBatcher) Stop() {
	b.processor.Stop()
}

// flush 批量落库（批内同连接去重由仓储层 BatchMarkDisconnected 统一处理）
func (b *DisconnectionBatcher) flush(batch []*models.DisconnectionEntry) {
	if len(batch) == 0 {
		return
	}

	// 用 context.Background() 而非 hub ctx：
	// StopRecords 在 Hub cancel 之前调用，但 flush 可能耗时较长，
	// 用独立 context 避免被 Hub cancel 截断（与心跳统计 flush 同款约定）
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if b.hub.GetConnectionRecordRepo() == nil {
		return
	}
	if err := b.hub.GetConnectionRecordRepo().BatchMarkDisconnected(ctx, batch); err != nil {
		b.hub.GetLogger().ErrorKV("批量标记连接断开失败",
			"error", err,
			"batch_size", len(batch),
		)
	}
}
