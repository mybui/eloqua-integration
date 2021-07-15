import settings
from app import app
from database import get_db
from eloqua_client import ElqClient
from logging_config import setup_logging

import argparse

logger = setup_logging(__name__)


def import_to_db(first_run):
    with app.app_context():
        db = get_db()
        client = ElqClient(username=settings.ELQ_USER,
                           password=settings.ELQ_PASSWORD,
                           base_url=settings.ELQ_BASE_URL)
        # insert all activities
        data, total_count = client.export_all_activities_with_contact(current_activity_data_in_db=db.get_all_activities_data())
        if data:
            db.insert_data(data=data, collection="allActivities")
            logger.debug("{0} Activity with Contact details imported to DB".format(total_count))
        else:
            logger.debug("No new Activity with Contact details imported to DB.")
        # insert PageView activity separately
        page_view_data, page_view_total_count = client.export_page_view_with_contact(first_run=first_run,
                                                                                     current_page_view_data_in_db=db.get_all_activities_data(type="PageView"))
        if page_view_data:
            db.insert_data(data=page_view_data, collection="allActivities")
            logger.debug("{0} PageView Activity with Contact details imported to DB".format(page_view_total_count))
        else:
            logger.debug("No new PageView Activity with Contact details imported to DB.")


if __name__ == '__main__':
    # add first run arg
    parser = argparse.ArgumentParser()
    parser.add_argument("--first_run", type=int, help="input 1 for first run, default is 0", default=0)
    first_run = parser.parse_args().first_run
    import_to_db(first_run=first_run)
