import sys
import argparse
import logging
import re
from datetime import datetime
import asyncio

from google.oauth2 import service_account
from googleapiclient import discovery


class BackupManager():
    """Check instances infomation,
       create new snapshots for backup enabled disks,
       apply retention policy to manage backups.
    """
    def __init__(self, compute) -> None:
        """Initialize the BackupManager
        """
        self.compute = compute
        self.instances = []
        self.snapshots = []
        self.project = 'devops-assessment-yixian'
        self.zone = 'europe-west4-a'
        self.task = None
        self.log = None

    def list_instances_and_snapshots(self):
        """Listing instances and snapshots in the project.
        """
        # compute.instance.list returns an HttpRequest, use execute to execute the request
        result = self.compute.instances().list(project=self.project, zone=self.zone).execute()
        self.instances = result['items'] if 'items' in result else None

        result = self.compute.snapshots().list(project=self.project).execute()
        self.snapshots = result['items'] if 'items' in result else None

    def get_last_backup_timestamp(self, snapshots, timestamp='creationTimestamp'):
        """Compare the timestamps to find out the last backup time.

        Args:
            snapshots (list(dict)): A list of snapshots dictionary
            timestamp (str, optional): Name of the key that contains the timestamp value in the snapshots' dictionary.
                                       Defaults to 'creationTimestamp'.

        Returns:
            str: The lastest timestamp string
        """
        last_backup_timestamp = snapshots[0][timestamp]
        for snapshot in snapshots[1:]:
            if last_backup_timestamp < snapshot[timestamp]:
                last_backup_timestamp = snapshot[timestamp]
        return last_backup_timestamp

    def get_instances_info(self):
        """Get the running instances and print out the results.
        """
        # Get the list of instances and snapshots
        self.list_instances_and_snapshots()

        # Print out the instances value in table format
        print('{:<20} {:<20} {:<20} {:<20}'.format('Instance', 'Backup Enabled', 'Disk', 'Last Backup'))
        # Check each instances and print out the desired values.
        for instance in self.instances:
            # If backup is enabled for an instance, get the timestamp for the lastest backup
            if instance['labels']['backup'] == 'true':
                last_backup = self.get_last_backup_timestamp(snapshots=self.snapshots)
            # If backup is not enabled for an instance, set last backup time to 'Never'
            else:
                last_backup = 'Never'
            print('{:<20} {:<20} {:<20} {:20}'.format(instance['name'], instance['labels']['backup'],
                                                      instance['name'], last_backup))

    def get_date_time(self, time_str):
        """Use regular expression to find the date and time in the time string.

        Args:
            time_str (str): The original timestamp string containing date and time

        Returns:
            str: A string for date
            str: A string for time
        """
        date_time = re.compile(r'(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2}.\d{3})').match(time_str)
        date = date_time.group(1)
        time = date_time.group(2)
        return date, time

    async def create_snapshot(self, instance, name):
        """Create a new disk snapshot.

        Args:
            instance (dict): A instance's dictionary
            name (str): name of the new snapshot
        """
        self.log.info('Starting asynchronous backup creation')
        new_snapshot = self.compute.disks().createSnapshot(project=self.project, zone=self.zone,
                                                           disk=instance['name'], body={'name': name}).execute()
        # With this await, the program will go back to execute the manage_snapshot method
        await asyncio.sleep(0.1)

        # Use zoneOperations to check the status of creating the new snapshots
        while True:
            # Start and wait get_zone_operations asynchronously
            status = await asyncio.create_task(self.get_zone_operations(new_snapshot, instance))
            # If the return status is 'DONE', the new snapshot has been created, return back to manage_snapshot method
            if status == 'DONE':
                break
            await asyncio.sleep(5)

    async def get_zone_operations(self, snapshot, instance):
        """Use the zoneOperations to check the status of creating new snapshot.

        Args:
            snapshot (dict): A snapshot dictionary
            instance (dict): A instance dictionary

        Returns:
            str: Status of the zoneOperations
        """
        zone_op = self.compute.zoneOperations().get(project=self.project, zone=self.zone,
                                                    operation=snapshot['name']).execute()
        self.log.info(f'Snapshot for disk {instance["name"]} is Status.{zone_op["status"]}')
        return zone_op['status']

    async def manage_snapshot(self, name, cheat_create, cheat_skip):
        """Check if snapshot has been made today,
           if not, create snapshot for instances that has label 'backup' set to true.

        Args:
            name (str): Name of the new snapshot to be created
            cheat_create (bool): This cheat only for demonstration and testing purposes, default is 'False'.
                                 A small trick to get around the restriction of creating new snapshot.
                                 When enabled, the instance will be able to create new snapshot
                                 even if there's already a snapshot created prior today.
            cheat_skip (bool):  This cheat only for demonstration and testing purposes, default is 'False'.
                                A small trick to get around the restriction of skipping creating new snapshot.
                                When enabled, the instance will be able to skip creating new snapshot
                                even if there's no snapshot created today.
        """
        # Set up a logger to log the information
        self.log = self.create_logger('snapshot')
        self.log.info('Starting backup process')

        # Get the list of instances and snapshots
        self.list_instances_and_snapshots()
        self.log.info(f'Found {len(self.instances)} instances')

        for instance in self.instances:
            self.log.info(f'Instance: {instance["name"]}')
            self.log.info(f'Backup Enabled: {instance["labels"]["backup"]}')

            # If the backup is enabled for the instance, continue to check for snapshot
            if instance['labels']['backup'] == 'true':
                # Compare the timestamps to find out the last backup time
                last_backup = self.get_last_backup_timestamp(snapshots=self.snapshots)

                # Get the date and time string seperately
                date_str, last_backup_time = self.get_date_time(last_backup)
                self.log.info(f'Last backup was {last_backup_time} ago')

                # Covert date string to datetime object
                last_backup_date = datetime.strptime(date_str, '%Y-%m-%d').date()

                # If the last backup is created today, skip creating new snapshot
                # If cheat is True, will continue to create a new snapshot
                if (last_backup_date == datetime.today().date() or cheat_skip) and not cheat_create:
                    self.log.info('Skipping backup creation since the last backup is too recent')
                # If no backup has been made today, continue to create new snapshot
                else:
                    # Create new snapshat asynchronously
                    self.task = asyncio.create_task(self.create_snapshot(instance, name))
                    # This await will allow the program to execute the asynchronous task
                    await asyncio.sleep(0.1)
        # Wait for the create_snapshot method to complete
        if self.task is not None:
            await self.task
            self.log.info('All snapshots done')

        # Close the handles of the logger at the end of the execution
        self.cleanup_log_handler()

    def construct_disks_snapshots_dict(self):
        """Construct a dict containing some necessary disks snapshots value.

        Returns:
            dict: An dict of the form:
            {
                "<sourceDiskId>"{  # A dict of snapshots created for this disk id
                    "<snapshot creation date>": [  # A list of snapshots created on this date
                        "name" : "<snapshot name>",
                        "id": "<snapshot id>",
                        "time": "<snapshot creation timestamp>"
                    ]
                }
            }
        """
        disks = {}
        for snapshot in self.snapshots:
            disk_id = snapshot['sourceDiskId']
            date, _ = self.get_date_time(snapshot['creationTimestamp'])
            if disk_id not in disks:
                disks[disk_id] = {}
            if date not in disks[disk_id]:
                disks[disk_id][date] = []
            snapshot_dict = {
                "name": snapshot['name'],
                "id": snapshot['id'],
                "time": snapshot['creationTimestamp']
            }
            disks[disk_id][date].append(snapshot_dict)
        return disks

    def delete_older_backups(self, backups):
        """Delete older backups on the disk.

        Args:
            backups (list(dict)): A list of dictionary containing certain values for snapshots
        """
        # Get the timestamp for the last backup
        last_backup_time = self.get_last_backup_timestamp(snapshots=backups, timestamp='time')

        # If the backup's timestamp is prior to the last backup timestamp, delete the backup
        for backup in backups:
            if backup['time'] < last_backup_time:
                self.compute.snapshots().delete(project=self.project, snapshot=backup['name']).execute()
                self.log.info(f'Deleting snapshot {backup["id"]}')

    def apply_retention_policy(self):
        """Apply the retention policy for backup snapshots
        """
        self.log = self.create_logger('retention_policy')
        self.log.info('Checking backups against retention policy')

        # Get the list of instances and snapshots
        self.list_instances_and_snapshots()

        # Collect all the disks id that contains backups
        disks = self.construct_disks_snapshots_dict()

        today = datetime.today().date()

        # Check the backups in each of the disk id
        for disk in disks.keys():
            self.log.info(f'Checking backups for disk {disk}')
            # For each disk, create a list to store the backups in the past week
            last_week_backups = []

            # Check if there are duplicates for the disk on the same day / week
            for date in disks[disk]:
                # Check how many days ago were the backups created
                creation_date = datetime.strptime(date, '%Y-%m-%d').date()
                date_diff = (today - creation_date).days

                # Get the numbers of backups on the date
                snapshots_num = len(disks[disk][date])

                # For the last 7 days, check if more than 1 backup per day has been kept
                if date_diff < 7:
                    # If more than 1 backups per day is kept, delete the older ones
                    if snapshots_num > 1:
                        self.log.info(f'Found {snapshots_num} snapshots made between {date_diff - 1} and {date_diff} days ago')
                        self.delete_older_backups(disks[disk][date])
                # For this demo, only check the backups that are kept prior to the last 7 days
                elif date_diff < 14:
                    for backup in disks[disk][date]:
                        last_week_backups.append(backup)

            # If more than 1 backups is kept for last week, delete the older ones
            if len(last_week_backups) > 1:
                self.log.info(f'Found {len(last_week_backups)} snapshots made in last week')
                self.delete_older_backups(last_week_backups)

        # Close the handles of the logger at the end of the execution
        self.cleanup_log_handler()

    # Create logger to log info to the terminal
    def create_logger(self, name):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s  %(levelname)s   %(message)s',
            handlers=[logging.StreamHandler(sys.stdout)]
        )
        return logging.getLogger(name)

    # Close the logging handlers
    def cleanup_log_handler(self):
        for handler in self.log.handlers:
            handler.close()
            self.log.removeFilter(handler)


# Parsing arguments
def create_parser(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('option',
                        help='''choose an option to run this program,
                        select from 'instances', 'snapshot', 'apply-retention-policy'.''')
    parser.add_argument('-n', '--name', type=str, default='new-snapshot', help='provide a name for a new snapshot')
    parser.add_argument('-cc', '--cheat_create', action='store_true',
                        help='Cheat to get around restrictions in the program, only for demonstration purposes.')
    parser.add_argument('-cs', '--cheat_skip', action='store_true',
                        help='Cheat to get around restrictions in the program, only for demonstration purposes.')
    return parser.parse_args(args)


def main(sys_args):
    # Get command line arguments
    args = create_parser(sys_args)

    # Authentication using the service accout file
    credentials = service_account.Credentials.from_service_account_file('devops-assessment-yixian.json')

    # Create a resource object for interacting with the service
    compute = discovery.build('compute', 'v1', credentials=credentials)
    # Create an object for the BackupManager
    backup = BackupManager(compute)
    try:
        # Case Backup-1: list the info of the virtual machines
        if args.option == 'instances':
            backup.get_instances_info()

        # Case Backup-2: Create snapshot for disks with 'backup' set to 'true'
        elif args.option == 'snapshot':
            asyncio.run(backup.manage_snapshot(args.name, args.cheat_create, args.cheat_skip))

        # Case Backup-3: Remove old backups following retention policy
        elif args.option == 'apply-retention-policy':
            backup.apply_retention_policy()
        else:
            raise Exception(f"'{args.option}' option not available, please select from 'instances', 'snapshot', 'apply-retention-policy'")
    except KeyboardInterrupt:
        sys.exit(0)

    return 0


if __name__ == '__main__':
    main(sys.argv[1:])
