package scheduler

import (
	"context"
	"fmt"
	"sync"

	syncservice "github.com/asachs01/autotask-data-sink/internal/sync"
	"github.com/asachs01/autotask-data-sink/pkg/config"
	"github.com/robfig/cron/v3"
	"github.com/rs/zerolog/log"
)

// Scheduler handles the scheduling of sync tasks
type Scheduler struct {
	syncService *syncservice.Service
	cron        *cron.Cron
	config      *config.SyncConfig
	ctx         context.Context
	cancel      context.CancelFunc
}

// New creates a new scheduler
func New(syncService *syncservice.Service, config *config.SyncConfig) *Scheduler {
	ctx, cancel := context.WithCancel(context.Background())

	return &Scheduler{
		syncService: syncService,
		cron:        cron.New(cron.WithSeconds()),
		config:      config,
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Start starts the scheduler
func (s *Scheduler) Start() error {
	// Schedule company sync
	if s.config.Companies > 0 {
		schedule := fmt.Sprintf("0 0 */%d * * *", s.config.Companies/60) // Convert minutes to hours
		_, err := s.cron.AddFunc(schedule, func() {
			if err := s.syncService.SyncCompanies(s.ctx); err != nil {
				log.Error().Err(err).Msg("Failed to sync companies")
			}
		})
		if err != nil {
			return fmt.Errorf("failed to schedule company sync: %w", err)
		}
		log.Info().
			Str("schedule", schedule).
			Msg("Scheduled company sync")
	}

	// Schedule contact sync
	if s.config.Contacts > 0 {
		schedule := fmt.Sprintf("0 0 */%d * * *", s.config.Contacts/60) // Convert minutes to hours
		_, err := s.cron.AddFunc(schedule, func() {
			if err := s.syncService.SyncContacts(s.ctx); err != nil {
				log.Error().Err(err).Msg("Failed to sync contacts")
			}
		})
		if err != nil {
			return fmt.Errorf("failed to schedule contact sync: %w", err)
		}
		log.Info().
			Str("schedule", schedule).
			Msg("Scheduled contact sync")
	}

	// Schedule ticket sync
	if s.config.Tickets > 0 {
		schedule := fmt.Sprintf("0 */%d * * * *", s.config.Tickets) // Every N minutes
		_, err := s.cron.AddFunc(schedule, func() {
			if err := s.syncService.SyncTickets(s.ctx); err != nil {
				log.Error().Err(err).Msg("Failed to sync tickets")
			}
		})
		if err != nil {
			return fmt.Errorf("failed to schedule ticket sync: %w", err)
		}
		log.Info().
			Str("schedule", schedule).
			Msg("Scheduled ticket sync")
	}

	// Start the cron scheduler
	s.cron.Start()
	log.Info().Msg("Scheduler started")

	return nil
}

// Stop stops the scheduler
func (s *Scheduler) Stop() {
	s.cancel()
	s.cron.Stop()
	log.Info().Msg("Scheduler stopped")
}

// RunInitialSync runs an initial sync for all entities
func (s *Scheduler) RunInitialSync() error {
	log.Info().Msg("Running initial sync")

	// Use a WaitGroup to wait for all syncs to complete
	var wg sync.WaitGroup

	// Channel to collect errors from goroutines
	errChan := make(chan error, 5) // Buffer for up to 5 errors

	// Run company sync in a goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.syncService.SyncCompanies(s.ctx); err != nil {
			log.Error().Err(err).Msg("Failed to sync companies")
			errChan <- fmt.Errorf("failed to sync companies: %w", err)
		}
	}()

	// Run contact sync in a goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.syncService.SyncContacts(s.ctx); err != nil {
			log.Error().Err(err).Msg("Failed to sync contacts")
			errChan <- fmt.Errorf("failed to sync contacts: %w", err)
		}
	}()

	// Run ticket sync in a goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.syncService.SyncTickets(s.ctx); err != nil {
			log.Error().Err(err).Msg("Failed to sync tickets")
			errChan <- fmt.Errorf("failed to sync tickets: %w", err)
		}
	}()

	// Wait for all syncs to complete
	wg.Wait()

	// Close the error channel
	close(errChan)

	// Check if there were any errors
	var errs []error
	for err := range errChan {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		// Return the first error
		return errs[0]
	}

	return nil
}

// WaitForever blocks until the context is cancelled
func (s *Scheduler) WaitForever() {
	<-s.ctx.Done()
}

// RunOnce runs a one-time sync for all entities
func (s *Scheduler) RunOnce() error {
	log.Info().Msg("Running one-time sync")

	// Use a WaitGroup to wait for all syncs to complete
	var wg sync.WaitGroup

	// Channel to collect errors from goroutines
	errChan := make(chan error, 5) // Buffer for up to 5 errors

	// Run company sync in a goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.syncService.SyncCompanies(s.ctx); err != nil {
			log.Error().Err(err).Msg("Failed to sync companies")
			errChan <- fmt.Errorf("failed to sync companies: %w", err)
		}
	}()

	// Run contact sync in a goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.syncService.SyncContacts(s.ctx); err != nil {
			log.Error().Err(err).Msg("Failed to sync contacts")
			errChan <- fmt.Errorf("failed to sync contacts: %w", err)
		}
	}()

	// Run ticket sync in a goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.syncService.SyncTickets(s.ctx); err != nil {
			log.Error().Err(err).Msg("Failed to sync tickets")
			errChan <- fmt.Errorf("failed to sync tickets: %w", err)
		}
	}()

	// Wait for all syncs to complete
	wg.Wait()

	// Close the error channel
	close(errChan)

	// Check if there were any errors
	var errs []error
	for err := range errChan {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		// Return the first error
		return errs[0]
	}

	log.Info().Msg("One-time sync completed")
	return nil
}
