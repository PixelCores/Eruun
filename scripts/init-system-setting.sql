-- Eruun system_setting default bootstrap script
-- Usage:
--   mysql -h <host> -u <user> -p <database> < scripts/init-system-setting.sql
--
-- This script is idempotent and non-destructive:
-- - Inserts defaults when rows do not exist
-- - Keeps existing user-managed values on repeated runs
-- Included default records:
-- - nodeSelector
-- - rbacPolicies
-- - urlSecurityPolicy
-- - podRestartMonitor

START TRANSACTION;

INSERT INTO eruun_system_setting (`type`, `value`, `create_time`, `update_time`)
VALUES
  (
    'nodeSelector',
    JSON_OBJECT(
      'nodeSelector', JSON_OBJECT(),
      'affinity', JSON_OBJECT(),
      'tolerations', JSON_ARRAY()
    ),
    NOW(),
    NOW()
  ),
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
  ),
  (
    'rbacPolicies',
    JSON_ARRAY(
      JSON_OBJECT(
        'serviceAccount', 'default',
        'rules', JSON_ARRAY(
          JSON_OBJECT(
            'apiGroups', JSON_ARRAY(''),
            'resources', JSON_ARRAY('pods'),
            'verbs', JSON_ARRAY('get', 'list')
          )
        )
      )
    ),
    NOW(),
    NOW()
  )
ON DUPLICATE KEY UPDATE
  `type` = VALUES(`type`);

COMMIT;
