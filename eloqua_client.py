from datetime import datetime, timedelta

from dea.bulk.api import BulkClient
from dea.bulk.definitions import ExportDefinition
from dea.rest.api.cdo import RestCdoClient
from requests.auth import HTTPBasicAuth

import config
from logging_config import setup_logging

logger = setup_logging(__name__)


class ElqClient:
    def __init__(self, username, password, base_url):
        self.bulk_client = BulkClient(auth=HTTPBasicAuth(username=username, password=password), base_url=base_url)
        self.rest_client = RestCdoClient(auth=HTTPBasicAuth(username=username, password=password), base_url=base_url)

    def export_contact(self, date_from, date_to, region):
        contacts = []
        # define filter
        if date_from and date_to:
            filters = "'{0}'>'{1}' AND '{2}'<'{3}' AND '{4}'='{5}'".format("{{Contact.Field(C_DateCreated)}}",
                                                                           date_from,
                                                                           "{{Contact.Field(C_DateCreated)}}",
                                                                           date_to,
                                                                           "{{Contact.Field(C_IM_CRM_Security_Label1)}}",
                                                                           region)
        elif date_from:
            filters = "'{0}'>'{1}' AND '{2}'='{3}'".format("{{Contact.Field(C_DateCreated)}}",
                                                           date_from,
                                                           "{{Contact.Field(C_IM_CRM_Security_Label1)}}",
                                                           region)
        elif date_to:
            filters = "'{0}'<'{1}' AND '{2}'='{3}'".format("{{Contact.Field(C_DateCreated)}}",
                                                           date_to,
                                                           "{{Contact.Field(C_IM_CRM_Security_Label1)}}",
                                                           region)
        else:
            filters = "'{0}'='{1}'".format("{{Contact.Field(C_IM_CRM_Security_Label1)}}",
                                           region)
        fields = config.contact_export_def
        if filters:
            export_def = ExportDefinition(name="contact_export_def", fields=fields, filter=filters)
            contact_export = self.bulk_client.bulk_contacts.exports.create_export(export_def=export_def,
                                                                                  delete_export_on_close=True,
                                                                                  sync_limit=50000)
        else:
            export_def = ExportDefinition(name="contact_export_def", fields=fields)
            contact_export = self.bulk_client.bulk_contacts.exports.create_export(export_def=export_def,
                                                                                  delete_export_on_close=True,
                                                                                  sync_limit=50000)

        for items in contact_export:
            contacts.append(items)
        return contacts

    def export_contact_with_crm_id(self, first_run):
        field = config.contact_crm_id_export_def
        past_time, current_time = self.get_last_24_hours_date()
        if first_run:
            filter = "NOT '{0}'=''".format("{{Contact.Field(C_IM_CRM_Contact_ID1)}}")
        else:
            filter = "NOT '{0}'='' AND '{1}'>='{2}' AND '{3}'<'{4}'".format("{{Contact.Field(C_IM_CRM_Contact_ID1)}}",
                                                                            "{{Contact.Field(C_DateModified)}}",
                                                                            past_time,
                                                                            "{{Contact.Field(C_DateModified)}}",
                                                                            current_time)
        export_def = ExportDefinition(name="contact_crm_id_export_def", fields=field, filter=filter)
        contact_export = self.bulk_client.bulk_contacts.exports.create_export(export_def=export_def,
                                                                              delete_export_on_close=True,
                                                                              sync_limit=50000)
        return list(contact_export)

    def export_all_activities_with_contact(self, current_activity_data_in_db):
        activities = []
        count = 0
        activity_types = ["EmailSend", "EmailOpen", "EmailClickthrough", "Subscribe",
                          "Unsubscribe", "Bounceback", "FormSubmit", "WebVisit"]
        for activity_type in activity_types:
            logger.debug("Start exporting data for {0} Activity with Contact details".format(activity_type))
            filter = "'{0}'='{1}' AND NOT '{2}'=''".format("{{Activity.Type}}",
                                                           activity_type,
                                                           "{{Activity.Contact.Field(C_IM_CRM_Contact_ID1)}}")
            fields = config.activity_with_contact_export_def[activity_type]
            export_def = ExportDefinition(name=activity_type.lower() + "_with_contact_export_def", fields=fields,
                                          filter=filter)
            activity_export = list(self.bulk_client.bulk_activities.exports.create_export(export_def=export_def,
                                                                                          delete_export_on_close=True,
                                                                                          sync_limit=50000))
            if activity_export:
                for activity in activity_export:
                    if activity not in current_activity_data_in_db:
                        activities.append(activity)
                count += len(activity_export)
                logger.debug("Finish exporting {0} Activity with Contact details".format(activity_type))
            else:
                logger.debug("No {0} Activity with Contact details exported.".format(activity_type))
        return activities, count

    def export_page_view_with_contact(self, first_run, current_page_view_data_in_db):
        output = []
        past_time, current_time = self.get_last_24_hours_date()
        field = config.activity_with_contact_export_def["PageView"]
        # export all PageView for contacts that got updated in last 24 hours
        contacts = self.export_contact_with_crm_id(first_run=first_run)
        if contacts:
            for contact in contacts:
                contact_id = contact.get("id", None)
                filter = "'{0}'='{1}' AND '{2}'='{3}'".format("{{Activity.Contact.Id}}", contact_id,
                                                              "{{Activity.Type}}", "PageView")
                export_def = ExportDefinition(name="pageview_with_crm_id_export_def", fields=field, filter=filter)
                page_view_export = self.bulk_client.bulk_activities.exports.create_export(export_def=export_def,
                                                                                          delete_export_on_close=True,
                                                                                          sync_limit=50000)
                for i in page_view_export:
                    output.append(i)
            logger.debug("Finish exporting PageView Activity with Contact details "
                         "(for contacts updated from {0} to {1}".format(past_time, current_time))
        else:
            logger.debug("No PageView Activity with Contact details exported. "
                         "No contacts updated from {0} to {1}".format(past_time, current_time))
        # export all PageView for the last 24 hours (for all contacts either recently updated or not)
        filter_page_view_by_day = "'{0}'='{1}' AND '{2}'>='{3}' AND '{4}'<'{5}'".format("{{Activity.Type}}",
                                                                                        "PageView",
                                                                                        "{{Activity.CreatedAt}}",
                                                                                        past_time,
                                                                                        "{{Activity.CreatedAt}}",
                                                                                        current_time)
        export_def_by_day = ExportDefinition(name="pageview_with_crm_id_export_def", fields=field,
                                             filter=filter_page_view_by_day)
        page_view_by_day = self.bulk_client.bulk_activities.exports.create_export(export_def=export_def_by_day,
                                                                                  delete_export_on_close=True,
                                                                                  sync_limit=50000)
        for page_view in page_view_by_day:
            if page_view.get("C_IM_CRM_Contact_ID1", None) and \
                    page_view not in output and \
                    page_view not in current_page_view_data_in_db:
                output.append(page_view)
        return output, len(output)

    def get_last_24_hours_date(self):
        current = datetime.utcnow().date()
        past = current - timedelta(days=1)
        return str(past), str(current)