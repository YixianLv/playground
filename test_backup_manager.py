import pytest
import subprocess
import time


# Helper functions

def run(args, timeout=60):
    process = subprocess.Popen(
        ['python3', '-m', 'backup_manager'] + args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
        )

    stdout, stderr = process.communicate(timeout=timeout)

    return {
        'stdout': stdout.decode().replace('\r', ''),
        'stderr': stderr.decode().replace('\r', ''),
        'status': process.returncode
    }


# Test backup-1: get instances info
def test_get_instances_info():
    data = run(['instances'])

    assert 'Instance' in data['stdout']
    assert 'backup-instance' in data['stdout']
    assert 'non-backup-instance' in data['stdout']
    assert 'true' in data['stdout']
    assert 'false' in data['stdout']
    assert 'Never' in data['stdout']
    assert '-08:00' in data['stdout']
    assert data['status'] == 0


# Test backup-2: Create snapshot for backup enabled disks

# Case: Creating new snapshot
def test_create_snapshot():
    data = run(['snapshot'])

    # If there are snapshots made prior today,
    # use --check_create to trick the program to create new snapshot
    if 'Skipping' in data['stdout']:
        data = run(['snapshot', '--cheat_create'])

    assert 'asynchronous backup creation' in data['stdout']
    assert 'Status.RUNNING' in data['stdout']
    assert 'Status.DONE' in data['stdout']
    assert 'All' in data['stdout']
    assert data['status'] == 0


# Case: Skip creating new snapshot
def test_skip_snapshot_creation():
    data = run(['snapshot'])

    # If no snapshot was made today,
    # use --check_skip to trick the program to skip the snapshot creation
    if 'Status' in data['stdout']:
        data = run(['snapshot', '--cheat_skip'])

    assert 'Skipping' in data['stdout']
    assert data['status'] == 0


# Test backup-3
# Case: Apply retention policy
def test_apply_retention_policy_remove():
    # Ensure there's backups to be removed
    run(['snapshot', '--cheat_create', '--name', 'test-1'])
    time.sleep(0.5)
    run(['snapshot', '--cheat_create', '--name', 'test-2'])
    time.sleep(0.5)

    data = run(['apply-retention-policy'])

    assert 'retention policy' in data['stdout']
    assert 'disk' in data['stdout']
    assert 'Found' in data['stdout']
    assert 'Deleting snapshot' in data['stdout']
    assert data['status'] == 0


# Case: No backups to be removed
def test_apply_retention_policy_non_remove():
    data = run(['apply-retention-policy'])

    assert 'retention policy' in data['stdout']
    assert 'disk' in data['stdout']
    assert 'Found' not in data['stdout']
    assert 'Deleting snapshot' not in data['stdout']
    assert data['status'] == 0


def test_option_error():
    data = run(['hi'])
    assert "'hi' option not available" in data['stderr']
    assert data['status'] != 0
