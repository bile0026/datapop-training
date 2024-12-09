import csv

from nautobot.apps.jobs import FileVar, BooleanVar
from nautobot.core.celery import register_jobs
from nautobot.extras.jobs import Job
from nautobot.dcim.models import Location, LocationType
from nautobot.extras.models import Status
from io import StringIO

import csv


class ImportLocations(Job):
    """Job to import locations from a custom CSV file."""

    def __init__(self, *args, **kwargs):
        """Initialize SSoTSyncDevices."""
        super().__init__(*args, **kwargs)

    class Meta:
        name = "Import Locations"
        description = "Import locations into Nautobot from a CSV file."
        has_sensitive_variables = False
        enabled = True

    csv_file = FileVar(
        label="CSV File",
        required=True,
        description="Upload your CSV file containing locations",
    )

    def run(self, csv_file, *args, **kwargs):
        state_map = {
            "CO": "Colorado",
            "VA": "Virginia",
            "CA": "California",
            "NJ": "New Jersey",
            "IL": "Illinois",
        }

        active_status, _ = Status.objects.get_or_create(name="Active")
        self.csv_file = csv_file
        decoded_csv = self.csv_file.read().decode("utf-8")
        csv_file = csv.DictReader(StringIO(decoded_csv))

        for row in csv_file:
            site_name = row["name"]
            city = row["city"]
            state_abbr = row["state"]

            state = state_map.get(state_abbr, state_abbr)

            if site_name.endswith("-DC"):
                location_type = "Data Center"
            elif site_name.endswith("-BR"):
                location_type = "Branch"
            else:
                self.job.logger.error(
                    f"Site name '{site_name}' does not end with '-DC' or '-BR'. Skipping."
                )
                continue

            state, _ = Location.objects.get_or_create(
                name=state,
                status=active_status,
                defaults={
                    "location_type": LocationType.objects.get(name="State")
                },
            )

            city, _ = LocationType.objects.get_or_create(
                name=city,
                status=active_status,
                defaults={
                    "location_type": LocationType.objects.get(name="City")
                },
            )

            site, created = Location.objects.update_or_create(
                name=site_name,
                status=active_status,
                defaults={
                    "location_type": LocationType.objects.get(name=location_type),
                },
            )

            if created:
                self.job.logger(f"Created site: {site_name}")
            else:
                self.log_info(f"Updated site: {site_name}")


jobs = [ImportLocations]
register_jobs(*jobs)
