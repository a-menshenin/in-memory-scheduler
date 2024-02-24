package inmemoryscheduler

import (
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

var InputData []interface{}

type (
	Schedule string
	Task     struct {
		taskName    string
		startTime   time.Time
		runInterval *time.Duration
		payload     []interface{}
	}
	TaskType          string
	InMemoryScheduler struct {
		workerPoolCount int
		taskCh          chan Task
		closeCh         chan struct{}
		scheduleTicker  *time.Ticker
		wg              *sync.WaitGroup
		logger          zap.Logger
		handlers        map[string]handler
	}

	handler func(payload []interface{}) error
)

func NewTask(
	taskName string,
	startDate time.Time,
	payload []interface{},
	runInterval *time.Duration,
) (*Task, error) {
	return &Task{taskName, startDate, runInterval, payload}, nil
}

func (t *Task) GetRunInterval() *time.Duration {
	return t.runInterval
}

func NewInMemoryScheduler(workerPoolCount int) *InMemoryScheduler {
	logger, _ := zap.NewProduction()

	s := &InMemoryScheduler{
		workerPoolCount,
		make(chan Task, workerPoolCount),
		make(chan struct{}),
		time.NewTicker(1 * time.Second),
		&sync.WaitGroup{},
		*logger,
		make(map[string]handler),
	}

	s.wg.Add(s.workerPoolCount)

	for i := 0; i < s.workerPoolCount; i++ {
		go s.worker()
	}

	return s
}

func (s *InMemoryScheduler) RegisterHandler(TaskName string, handler func(payload []interface{}) error) {
	s.handlers[TaskName] = handler
}

func (s *InMemoryScheduler) UnregisterHandler(TaskName string, handler func(payload []interface{}) error) {
	delete(s.handlers, TaskName)
}

func (s *InMemoryScheduler) AddTask(task Task) {
	s.taskCh <- task
}

func (s *InMemoryScheduler) Close() {
	for i := 0; i < s.workerPoolCount; i++ {
		s.closeCh <- struct{}{}
	}
	s.wg.Wait()
	s.scheduleTicker.Stop()

	for t := range s.taskCh {
		s.logger.Warn("Task not handled", zap.Any("task", t))
	}
}

func (s *InMemoryScheduler) worker() {
	s.logger.Error("worker started")
	defer func() {
		s.logger.Error("worker stopped")
		s.wg.Done()
	}()

	for {
		select {
		case <-s.closeCh:
			fmt.Println("Receive from closeCh")
			return
		case <-s.scheduleTicker.C:
			currentTime := time.Now()
			t := <-s.taskCh

			// Если время начала таски позже текущего, то ложим обратно и пропускаем
			if t.startTime.After(currentTime) {
				s.taskCh <- t

				continue
			}

			h, handlerExists := s.handlers[t.taskName]
			if !handlerExists {
				s.logger.Sugar().Errorf("Handler for task \"%s\" does not exist", t.taskName)

				continue
			}

			err := h(t.payload)
			if err != nil {
				s.logger.Error("Task error", zap.Any("task", t), zap.Error(err))
			} else {
				s.logger.Info("OneTimeTask handled")
			}

			// Если интервал передан, то прибавляем к текущему времени после исполнения интервал и снова ложим в канал с тасками
			if t.runInterval != nil {
				t.startTime = time.Now().Add(*t.runInterval)
				s.taskCh <- t
			}
		}
	}
}
