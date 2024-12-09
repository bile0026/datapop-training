import csv
import logging

from nautobot.apps.jobs import FileVar, BooleanVar
from nautobot.core.celery import register_jobs
from nautobot.extras.jobs import Job
from nautobot.dcim.models import Location, LocationType
from nautobot.extras.models import Status
from io import StringIO

LOGGER = logging.getLogger(__name__)
name = "Device Onboarding"  # pylint: disable=invalid-name


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
            city_name = row["city"]
            state_abbr = row["state"]

            normalized_state = state_map.get(state_abbr, state_abbr)

            if site_name.endswith("-DC"):
                location_type = "Data Center"
            elif site_name.endswith("-BR"):
                location_type = "Branch"
            else:
                self.job.logger.error(
                    f"Site name '{site_name}' does not end with '-DC' or '-BR'. Skipping."
                )
                continue

            self.logger.info(f"Creating/Checking state {normalized_state}")
            state, _ = Location.objects.get_or_create(
                name=normalized_state,
                status=active_status,
                defaults={
                    "location_type": LocationType.objects.get(name="State")
                },
            )

            self.logger.info(f"Creating/Checking city {city_name}")
            city, _ = Location.objects.get_or_create(
                name=city_name,
                status=active_status,
                parent=state,
                defaults={
                    "location_type": LocationType.objects.get(name="City")
                },
            )

            self.logger.info(f"Creating/Checking site {site_name}")
            site, created = Location.objects.update_or_create(
                name=site_name,
                status=active_status,
                parent=city,
                defaults={
                    "location_type": LocationType.objects.get(name=location_type),
                },
            )

            if created:
                self.logger.info(f"Created site: {site_name}")
            else:
                self.logger.info(f"Updated site: {site_name}")


jobs = [ImportLocations]
register_jobs(*jobs)
