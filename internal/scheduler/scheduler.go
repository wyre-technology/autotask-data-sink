package scheduler

import (
	"context"
	"fmt"

	"github.com/asachs01/autotask-data-sink/internal/sync"
	"github.com/asachs01/autotask-data-sink/pkg/config"
	"github.com/robfig/cron/v3"
	"github.com/rs/zerolog/log"
)

// Scheduler handles the scheduling of sync tasks
type Scheduler struct {
	syncService *sync.Service
	cron        *cron.Cron
	config      *config.SyncConfig
	ctx         context.Context
	cancel      context.CancelFunc
}

// New creates a new scheduler
func New(syncService *sync.Service, config *config.SyncConfig) *Scheduler {
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

	// Sync companies
	if err := s.syncService.SyncCompanies(s.ctx); err != nil {
		return fmt.Errorf("failed to sync companies: %w", err)
	}

	// Sync contacts
	if err := s.syncService.SyncContacts(s.ctx); err != nil {
		return fmt.Errorf("failed to sync contacts: %w", err)
	}

	// Sync tickets
	if err := s.syncService.SyncTickets(s.ctx); err != nil {
		return fmt.Errorf("failed to sync tickets: %w", err)
	}

	log.Info().Msg("Initial sync completed")
	return nil
}

// WaitForever blocks until the context is cancelled
func (s *Scheduler) WaitForever() {
	<-s.ctx.Done()
}

// RunOnce runs a single sync for all entities
func (s *Scheduler) RunOnce() error {
	log.Info().Msg("Running one-time sync")

	// Sync companies
	if err := s.syncService.SyncCompanies(s.ctx); err != nil {
		return fmt.Errorf("failed to sync companies: %w", err)
	}

	// Sync contacts
	if err := s.syncService.SyncContacts(s.ctx); err != nil {
		return fmt.Errorf("failed to sync contacts: %w", err)
	}

	// Sync tickets
	if err := s.syncService.SyncTickets(s.ctx); err != nil {
		return fmt.Errorf("failed to sync tickets: %w", err)
	}

	log.Info().Msg("One-time sync completed")
	return nil
}
