package handlers

import (
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"time"

	"bench-server/pkg/database"

	"github.com/sirupsen/logrus"
)

// Server HTTP服务器结构
type Server struct {
	db          *sql.DB
	logger      *logrus.Logger
	cache       []*database.SensorData // 数据缓存
	cacheMutex  sync.Mutex             // 保护缓存的互斥锁
	flushTicker *time.Ticker           // 定时刷新缓存的计时器
	stopChan    chan struct{}          // 停止后台goroutine的通道
}

// NewServer 创建新的服务器实例
func NewServer(db *sql.DB, logger *logrus.Logger) *Server {
	server := &Server{
		db:       db,
		logger:   logger,
		cache:    make([]*database.SensorData, 0),
		stopChan: make(chan struct{}),
	}

	// 启动后台goroutine定期刷新缓存
	server.flushTicker = time.NewTicker(1 * time.Second)
	go server.backgroundFlush()

	return server
}

// Stop 停止服务器，清理资源
func (s *Server) Stop() {
	s.flushTicker.Stop()
	close(s.stopChan)
	s.flushCache() // 确保最后的数据被刷新
}

// backgroundFlush 后台goroutine，定期刷新缓存到数据库
func (s *Server) backgroundFlush() {
	for {
		select {
		case <-s.flushTicker.C:
			s.flushCache()
		case <-s.stopChan:
			return
		}
	}
}

// flushCache 将缓存中的数据批量写入数据库
func (s *Server) flushCache() {
	s.cacheMutex.Lock()
	defer s.cacheMutex.Unlock()

	if len(s.cache) == 0 {
		return
	}

	// 复制当前缓存并清空
	dataToInsert := make([]*database.SensorData, len(s.cache))
	copy(dataToInsert, s.cache)
	s.cache = s.cache[:0]

	// 批量插入数据
	dbService := database.NewService(s.db)
	if err := dbService.BatchInsertSensorData(dataToInsert); err != nil {
		s.logger.WithError(err).Error("Failed to batch insert sensor data")
		// 如果插入失败，将数据重新放回缓存
		s.cache = append(s.cache, dataToInsert...)
		return
	}

	s.logger.Infof("Flushed %d records to database", len(dataToInsert))
}

// SensorDataHandler 处理传感器数据上报（扩展功能）
func (s *Server) SensorDataHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Only POST method allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var data database.SensorData
	if err := json.Unmarshal(body, &data); err != nil {
		http.Error(w, "Invalid JSON format", http.StatusBadRequest)
		return
	}

	// 数据验证
	if data.DeviceID == "" || data.MetricName == "" || data.Timestamp == "" {
		http.Error(w, "Missing required fields", http.StatusBadRequest)
		return
	}

	// 验证优先级
	if data.Priority < 1 || data.Priority > 3 {
		data.Priority = 2 // 默认中等优先级
	}

	// 将数据添加到缓存
	s.cacheMutex.Lock()
	s.cache = append(s.cache, &data)
	s.cacheMutex.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "success",
		"message": "Data cached successfully",
	})
}

// StatsHandler 处理统计信息请求
func (s *Server) StatsHandler(w http.ResponseWriter, r *http.Request) {
	dbService := database.NewService(s.db)
	stats, err := dbService.GetStats()
	if err != nil {
		s.logger.WithError(err).Error("Failed to get stats")
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}
