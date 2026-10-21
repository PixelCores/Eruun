-- Eruun system_setting default bootstrap script
-- Usage:
--   mysql -h <host> -u <user> -p <database> < scripts/init-system-setting.sql
--
-- This script is idempotent and non-destructive:
-- - Inserts defaults when rows do not exist
-- - Keeps existing user-managed values on repeated runs
-- Included default records:
-- - urlSecurityPolicy
-- - podRestartMonitor

START TRANSACTION;

INSERT INTO eruun_system_setting (`type`, `value`, `create_time`, `update_time`)
VALUES
  (
    'urlSecurityPolicy',
    JSON_OBJECT(
      'allowPrivateByDefault', FALSE,
      'allowedHostPatterns', JSON_ARRAY('*.svc.cluster.local', '*.paas.example.com'),
      'allowedCIDRs', JSON_ARRAY()
    ),
    NOW(),
    NOW()
  ),
  (
    'podRestartMonitor',
    JSON_OBJECT(
      'enabled', TRUE,
      'windowSeconds', 1800,
      'threshold', 3
    ),
    NOW(),
    NOW()
  )
ON DUPLICATE KEY UPDATE
  `type` = VALUES(`type`);

COMMIT;
