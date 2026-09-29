/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-09-15 13:30:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-09-15 13:56:00
 * @FilePath: \go-wsc\spi\connection_quality_store.go
 * @Description: 连接质量存储 SPI - ConnectionQualityStore 接口契约定义
 *
 * 连接运行时质量指标（心跳 Ping 统计、消息/字节计数、错误、评分）的持久化契约
 * GORM 实现见 adapter/gorm 包 ConnectionQualityStore
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */

package spi

import (
	"context"

	"github.com/kamalyes/go-wsc/models"
)

// ConnectionQualityStore 连接质量存储接口
// 承载运行时质量指标(心跳/消息/错误/重连)与评分，随 batcher 高频批量更新
type ConnectionQualityStore interface {
	// Upsert 创建或更新质量记录（首次连接建初始零值行 QualityScore=100，重连 reconnect_count+1）
	Upsert(ctx context.Context, quality *models.ConnectionQuality) error

	// BatchUpdateHeartbeats 批量更新 Ping 统计与活跃时间（单事务，心跳时间戳由 ConnectionStore 写 connect 表）
	BatchUpdateHeartbeats(ctx context.Context, entries []*models.HeartbeatUpdateEntry) error

	// BatchIncrementStats 批量递增消息/字节统计（单事务）
	BatchIncrementStats(ctx context.Context, entries []*models.StatsIncrementEntry) error

	// BatchAddErrors 批量记录连接错误（单事务）
	// 同一连接的多次错误由调用方在攒批 flush 时合并为一条 ErrorUpdateEntry（count 累加、保留最新错误）
	BatchAddErrors(ctx context.Context, entries []*models.ErrorUpdateEntry) error

	// BatchFinalizeOnDisconnect 批量断开终评（唯一入口，单条 FinalizeOnDisconnect 已废弃）
	// 停机路径逐连接终评 = 每连接 3 条 SQL（SELECT quality + SELECT duration + UPDATE score），
	// 26w 连接 = 78w 条 SQL 在停机窗口直打 DB，是滚动更新期 DB 过载的主源；
	// 批量化后 3 SQL/连接 → 每块 1 条 IN 读 + 1 条 CASE WHEN 写（Go 内算分），
	// duration 由调用方从 MarkDisconnectedBatch 返回的 entries 带入（与 SQL 写入值同源，无需读回）
	BatchFinalizeOnDisconnect(ctx context.Context, entries []*models.DisconnectionEntry) error

	// GetByConnectionID 根据连接ID获取质量记录
	GetByConnectionID(ctx context.Context, connectionID string) (*models.ConnectionQuality, error)

	// GetByUserID 根据用户ID获取所有质量记录
	GetByUserID(ctx context.Context, userID string) ([]*models.ConnectionQuality, error)

	// GetHighErrorRateConnections 获取高错误率连接
	GetHighErrorRateConnections(ctx context.Context, errorThreshold int, limit int) ([]*models.ConnectionQuality, error)

	// GetFrequentReconnectConnections 获取频繁重连的连接
	GetFrequentReconnectConnections(ctx context.Context, reconnectThreshold int, limit int) ([]*models.ConnectionQuality, error)

	// Close 关闭仓库
	Close() error
}
