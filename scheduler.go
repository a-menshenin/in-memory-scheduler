package inmemoryscheduler

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
)

var InputData []interface{}

const (
	OneTimeTaskType = "one_time"
	SheduleTaskType = "schedule"
)

type (
	Task struct {
		taskType string
		schedule string
		inputFunc func() error
		startDate time.Time
	}
	InMemoryScheduler struct {
		workerPoolCount int
		taskCh chan Task
		logger zap.Logger
	}
)

func NewTask(taskType string, schedule string, inputFunc func() error, startDate time.Time) *Task {
	return &Task{taskType, schedule, inputFunc, startDate}
}

func(t *Task) Handle() error {
	err := t.inputFunc()
	if err != nil {
		return err
	}

	return nil
}

func (t *Task) GetType() string {
	return t.taskType
}

func (t *Task) GetSchedule() string {
	return t.schedule
}

func NewInMemoryScheduler(workerPoolCount int) *InMemoryScheduler {
	logger, _ := zap.NewProduction()
	
	return &InMemoryScheduler{
		workerPoolCount,
		make(chan Task),
		*logger,
	}
}

func (s *InMemoryScheduler) AddTask(task Task) {
	s.taskCh <- task
}

func (s *InMemoryScheduler) AddTasks(tasks []Task) {
	for _, task := range tasks {
		s.taskCh <- task
	}
}

func (s *InMemoryScheduler) Run(ctx context.Context) {
	wg := &sync.WaitGroup{}
	wg.Add(s.workerPoolCount)

	quit := make(chan struct{})
	ch := make(chan Task, s.workerPoolCount)
	for i := 0; i < s.workerPoolCount; i++ {
		go s.worker(ctx, ch, wg, quit)
	}

	wg.Wait()

	s.logger.Error("All tasks handled!")
}

func (s *InMemoryScheduler) worker(ctx context.Context, taskCh chan Task, wg *sync.WaitGroup, quitCh chan struct{}) {
	s.logger.Error("worker started")
	defer func() {
		s.logger.Error("worker stopped")
		wg.Done()
	}()

	for {
		select {
		case <-quitCh:
			return
		case t := <-taskCh:
			wg.Add(1)
			go func() {
				defer wg.Done()
				switch t.GetType() {
				case OneTimeTaskType:
					err := t.Handle()
					if err != nil {
						s.logger.Sugar().Errorf("OneTimeTaskType error: %w", err)
					}

					s.logger.Info("OneTimeTask handled")
				}
			}()
		}
	}
}
