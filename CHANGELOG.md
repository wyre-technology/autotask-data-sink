# Changelog

## v1.1.0 release notes
2024-03-07

Updated the autotask-go library to v1.1.0 and added support for additional Autotask entity types.

## NEW FEATURES

- Added support for TimeEntries, Contracts, Projects, Tasks, and ConfigurationItems entities
- Added webhook registration functionality for real-time updates
- Updated autotask-go library from v1.0.0 to v1.1.0

## IMPROVEMENTS

- Replaced placeholder implementations with actual API calls for TimeEntries and Contracts
- Enhanced error handling and logging for API responses

## FIXES

- None

## v1.0.0 release notes
2023-03-06

Initial release of the Autotask Data Sink application.

## NEW FEATURES

- Initial implementation of the Autotask Data Sink
- Support for syncing companies, contacts, and tickets from Autotask API
- PostgreSQL database storage with historical tracking
- Configurable sync frequencies for different entity types
- Docker and docker-compose support for easy deployment
- Grafana integration for analytics and dashboards

## IMPROVEMENTS

- None (initial release)

## FIXES

- None (initial release)

## v1.1.1 release notes
2024-03-07

Fixed issues with Autotask API query filters and field mapping that were preventing company data from being retrieved and displayed correctly. Improved performance with concurrent entity syncing.

## NEW FEATURES

- Implemented concurrent syncing of entities using goroutines for improved performance

## IMPROVEMENTS

- Added helper methods for consistent API querying
- Enhanced error handling for API requests
- Improved sync performance by running company, contact, and ticket syncs concurrently

## FIXES

- Fixed API query filter format by using proper JSON array structure for filters
- Implemented direct API request construction to bypass library filter parsing issues
- Created specialized helper methods for empty filters and date-based filters
- Fixed field mapping for company data to use correct field names from the API (companyName instead of name)
- Updated address field mappings to match the actual API response structure
- Resolved filter format issues by implementing the exact format from the official Autotask API documentation
- Implemented alternative approach for retrieving companies, contacts, and tickets using individual GET requests instead of query endpoint
- Updated contact field mappings to use correct field names (isActive instead of active)
- Fixed contact sync issue by ensuring each contact has an ID field
- Added helper function for parsing time values from the API
- Added debug logging for API request bodies to aid in troubleshooting 