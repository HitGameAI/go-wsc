/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-09-29 10:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-09-29 10:00:00
 * @FilePath: \go-wsc\batcher\error_stats_batcher.go
 * @Description: 连接错误统计批量更新器
 *   基于 syncx.BatchProcessor 泛型批量处理器实现
 *   收集连接错误，按 batch flush 到 DB（单事务批量 UPDATE）
 *   同一 connectionID 的多次错误在 flush 时合并，减少事务内 UPDATE 条数
 *
 *   优化前：每次连接错误 → syncx.Go() → 1 次 DB UPDATE（AddError）
 *   断连风暴 3000 连接 = 3000 个 goroutine + 3000 次 DB 调用
 *   优化后：每次连接错误 → Submit（非阻塞） → BatchProcessor 收集 → 单事务批量 UPDATE
 *   断连风暴 3000 连接 = 3000 次 Submit（channel 写入）+ 1 次事务（合并后 ≤3000 条 UPDATE）
 *
 * 工作流程：
 *   1. 连接错误路径调用 Submit（非阻塞，队列满时丢弃）
 *   2. BatchProcessor 后台 worker 收集，满 batchSize 或每 flushInterval 触发 flush
 *   3. flush 时按 connectionID 聚合（次数累加、错误消息保留最新），单事务写入 quality 表
 *   4. SafeShutdown 时调用 Stop，flush 剩余数据后退出
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

// ErrorStatsItem 连接错误条目（Hub 侧构造）
type ErrorStatsItem struct {
	ConnectionID string
	Error        error
	ErrorAt      time.Time
}

// ErrorStatsBatcher 连接错误统计批量更新器
type ErrorStatsBatcher struct {
	hub       StorageBatchWriter
	processor *syncx.BatchProcessor[*ErrorStatsItem]
}

// NewErrorStatsBatcher 创建连接错误统计批量更新器
func NewErrorStatsBatcher(hub StorageBatchWriter, queueSize, batchSize int, flushInterval time.Duration) *ErrorStatsBatcher {
	u := &ErrorStatsBatcher{hub: hub}
	u.processor = syncx.NewBatchProcessor(queueSize, batchSize, flushInterval, u.flush)
	return u
}

// Submit 非阻塞提交连接错误
// 队列满时返回 false（错误统计可丢弃，非核心路径）
func (u *ErrorStatsBatcher) Submit(item *ErrorStatsItem) bool {
	return u.processor.Submit(item)
}

// Stop 停止更新器，flush 剩余数据后退出
func (u *ErrorStatsBatcher) Stop() {
	u.processor.Stop()
}

// flush 批量更新 DB
// 按 connectionID 聚合：ErrorCount 累加合并次数，LastError/LastErrorAt 保留最新一次错误
func (u *ErrorStatsBatcher) flush(batch []*ErrorStatsItem) {
	if len(batch) == 0 {
		return
	}

	// 按 connectionID 聚合，保留最新错误（ErrorAt 最大者）
	aggregated := make(map[string]*models.ErrorUpdateEntry, len(batch))
	for _, item := range batch {
		existing, ok := aggregated[item.ConnectionID]
		if !ok {
			aggregated[item.ConnectionID] = &models.ErrorUpdateEntry{
				ConnectionID: item.ConnectionID,
				ErrorCount:   1,
				LastError:    item.Error.Error(),
				LastErrorAt:  item.ErrorAt,
			}
		} else {
			existing.ErrorCount++
			if !item.ErrorAt.Before(existing.LastErrorAt) {
				existing.LastError = item.Error.Error()
				existing.LastErrorAt = item.ErrorAt
			}
		}
	}

	entries := make([]*models.ErrorUpdateEntry, 0, len(aggregated))
	for _, entry := range aggregated {
		entries = append(entries, entry)
	}

	// 用 context.Background() 而非 h.ctx
	// SafeShutdown 中 Stop() 在 h.cancel() 之前调用，但 flush 可能耗时较长
	// 用独立 context 避免被 Hub cancel 截断
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 质量仓储未注入时跳过（与 HeartbeatStatsUpdater 的 flush 防御一致）
	if repo := u.hub.GetConnectionQualityRepository(); repo != nil {
		if err := repo.BatchAddErrors(ctx, entries); err != nil {
			u.hub.GetLogger().ErrorContextKV(u.hub.Context(), "批量更新连接错误统计失败",
				"error", err,
				"batch_size", len(entries),
			)
		}
	}
}
