import math
import sys

from dea import BulkClient
from dea.bulk import CdoImportDefinition, ImportDefinition
from dea.bulk.api.cdo import BulkCdoClient
from dea.bulk.definitions import MapDataCardsConfig
from dea.bulk.eml import eml
from requests.auth import HTTPBasicAuth

import config
import settings
from app import app
from database import get_db
from logging_config import setup_logging
import re

logger = setup_logging(__name__)

es_pattern = re.compile(r"ES")
pt_pattern = re.compile(r"PT")
uk_pattern = re.compile(r"UK")
de_pattern = re.compile(r"DE")
region_labels = ["ES", "ES", "UK", "DE"]
region_patterns = [es_pattern, pt_pattern, uk_pattern, de_pattern]


def import_to_eloqua():
    with app.app_context():
        db = get_db()
        index = 0
        while index != len(region_labels):
            logger.debug("Start importing to Eloqua for region {0}".format(region_labels[index]))
            contacts = db.get_contact_data(region=region_patterns[index])
            activities = db.get_cdo_data(collection="activity", region=region_patterns[index])
            institutions = db.get_cdo_data(collection="institution", region=region_patterns[index])
            # import contacts
            if contacts:
                filtered_data = filter_new_data(db=db, collection="contactPast",
                                                current_data=contacts, region=region_patterns[index])
                if filtered_data:
                    bulk_import_contact(data=filtered_data, fields=config.contact_import_def)
                db.move_data_past(collection="contactPast", data=filtered_data, region=region_patterns[index])
            else:
                logger.debug("No Contact to import to Eloqua for region {0}, country {1}".format(region_labels[index],
                                                                                                      region_patterns[index].pattern))
            # import activities
            if activities:
                filtered_data = filter_new_data(db=db, collection="activityPast",
                                                current_data=activities, region=region_patterns[index])
                if filtered_data:
                    bulk_import_cdo(data=filtered_data, cdo_id=config.activity_cdo_id, fields=config.activity_import_def)
                db.move_data_past(collection="activityPast", data=filtered_data, region=region_patterns[index])
            else:
                logger.debug("No Activity CDO to import to Eloqua for region {0}, country {1}".format(region_labels[index],
                                                                                                           region_patterns[index].pattern))
                # delete current data if same with already imported, and keep past data
                db.delete_data(collection="activity", region=region_patterns[index])
                db.delete_data(collection="personActivity", region=region_patterns[index])
            # import institutions
            if institutions:
                filtered_data = filter_new_data(db=db, collection="institutionPast",
                                                current_data=institutions, region=region_patterns[index])
                if filtered_data:
                    bulk_import_cdo(data=filtered_data, cdo_id=config.institution_cdo_id, fields=config.institution_import_def)
                db.move_data_past(collection="institutionPast", data=filtered_data, region=region_patterns[index])
            else:
                logger.debug("No Institution CDO to import to Eloqua for region {0}, country {1}".format(region_labels[index],
                                                                                                              region_patterns[index].pattern))
                db.delete_data(collection="institution", region=region_patterns[index])
                db.delete_data(collection="personInstitution", region=region_patterns[index])
            logger.debug("Finish importing to Eloqua for region {0}".format(region_labels[index]))
            # increase index
            index += 1


def bulk_import_cdo(data, cdo_id, fields):
    dea_client = BulkCdoClient(auth=HTTPBasicAuth(username=settings.ELQ_USER, password=settings.ELQ_PASSWORD),
                               base_url=settings.ELQ_BASE_URL)
    # define different import def name
    import_def_name = "activity_cdo_import_def"
    if cdo_id == config.institution_cdo_id:
        import_def_name = "institution_cdo_import_def"
    # map to field IM CRM Contact ID
    map_data_card = MapDataCardsConfig(entity_type="Contact", entity_field=eml.Contact.Field("C_IM_CRM_Contact_ID1"),
                                       source_field="IM_CRM_Contact_ID")
    cdo_import_def = CdoImportDefinition(name=import_def_name, fields=fields,
                                         id_field_name="IM_CRM_Row_ID",
                                         parent_id=cdo_id,
                                         trigger_sync_on_import=True,
                                         map_data_cards=map_data_card)
    with dea_client.bulk_cdo.imports.create_import(import_def=cdo_import_def, parent_id=cdo_id) as bulk_import:
        bulk_import.add_items(data)
        bulk_import.upload_and_flush_data(sync_on_upload=True)
    logger.debug("Bulk CDO import complete")
    if import_def_name == "activity_cdo_import_def":
        logger.debug("{0} Activity CDO imported to Eloqua".format(len(data)))
    if import_def_name == "institution_cdo_import_def":
        logger.debug("{0} Institution CDO imported to Eloqua".format(len(data)))


def bulk_import_contact(data, fields):
    dea_client = BulkClient(auth=HTTPBasicAuth(username=settings.ELQ_USER, password=settings.ELQ_PASSWORD),
                            base_url=settings.ELQ_BASE_URL)
    contact_import_def = ImportDefinition(name="contact_import_def", fields=fields,
                                          id_field_name="C_EmailAddress")
    with dea_client.bulk_contacts.imports.create_import(import_def=contact_import_def) as bulk_import:
        bulk_import.add_items(data)
        bulk_import.upload_and_flush_data(sync_on_upload=True)
    logger.debug("Bulk Contact import complete")
    logger.debug("{0} Contact imported to Eloqua".format(len(data)))


def filter_new_data(db, collection, current_data, region):
    # get past data
    past_data = []
    if collection == "contactPast":
        past_data = db.get_past_data(collection="contactPast", region=region)
    if collection == "activityPast":
        past_data = db.get_past_data(collection="activityPast", region=region)
    if collection == "institutionPast":
        past_data = db.get_past_data(collection="institutionPast", region=region)
    if past_data:
        # compare to get new data (different from past) only
        filtered_data = [item for item in current_data if item not in past_data]
        logger.debug("Filter new data vs. collection {0} complete with {1} new data compared to yesterday".format(collection, len(filtered_data)))
        return filtered_data
    logger.debug("Filter new data complete with all new data compared to {0} collection".format(collection))
    return current_data


if __name__ == '__main__':
    import_to_eloqua()
