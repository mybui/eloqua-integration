from flask import Flask, jsonify, request
from flask_httpauth import HTTPBasicAuth

import settings
from database import get_db
from eloqua_client import ElqClient
from logging_config import setup_logging
from schema_validator import check_data_schema, activity_fields, institution_fields, person_activity_fields, \
    person_institution_fields, contact_fields

logger = setup_logging(__name__)
app = Flask(__name__)
app.config.from_object("settings")
app.logger.debug("configured Flask app")
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
auth = HTTPBasicAuth()

client = ElqClient(username=settings.ELQ_USER, password=settings.ELQ_PASSWORD, base_url=settings.ELQ_BASE_URL)


@app.route('/status', methods=["GET"])
@auth.login_required()
def status():
    return "Up and running", 200


@app.route('/activity', methods=["GET", "POST"])
@auth.login_required()
def activity():
    if request.method == "POST":
        data = request.get_json(force=True)
        # validate data schema
        checked_data = check_data_schema(data=data, fields=activity_fields)
        if checked_data:
            db = get_db()
            output = db.insert_data(collection="activity", data=checked_data)
            if output:
                return jsonify({"success": True}), 201
        else:
            return "The service has encountered an error. Please make sure the data sent has correct fields. \n" \
                   "Accepted fields are: \n " \
                   "For testing purposes, the service currently only accepts data from ES or UK or DE." \
                   "{0}".format(activity_fields), 500
    elif request.method == "GET":
        db = get_db()
        activity_date_from = request.args.get("dateFrom", "")
        activity_date_to = request.args.get("dateTo", "")
        region = request.args.get("label", "")
        limit = int(request.args.get("limit", 20000))
        offset = int(request.args.get("offset", 0))
        if limit <= 20000:
            response = db.query_all_activities_data(activity_date_from=activity_date_from,
                                                    activity_date_to=activity_date_to,
                                                    region=region)
            if response:
                if offset + len(response[offset:limit + offset]) < len(response):
                    has_more = True
                else:
                    has_more = False
                formatted = {"items": response[offset:limit + offset],
                             "totalResults": len(response),
                             "limit": limit,
                             "offset": offset,
                             "count": len(response[offset:limit + offset]),
                             "has more": has_more}
                return jsonify(formatted), 200
            else:
                formatted = {"items": response,
                             "totalResults": 0,
                             "limit": limit,
                             "offset": 0,
                             "count": 0,
                             "has more": False}
                return jsonify(formatted), 200
        else:
            return jsonify({
                "failures": [
                    {
                        "field": "limit",
                        "value": "1000000",
                        "constraint": "Must be a positive integer value, at most 1000000, if specified."
                    }
                ]
            }), 400
    else:
        return "Method not allowed", 404


@app.route('/activity/contact', methods=["GET"])
@auth.login_required()
def activity_with_contact():
    db = get_db()
    contact_date_from = request.args.get("dateFrom", "")
    contact_date_to = request.args.get("dateTo", "")
    region = request.args.get("label", "")
    limit = int(request.args.get("limit", 20000))
    offset = int(request.args.get("offset", 0))
    if limit <= 20000:
        response = db.query_all_activities_data_with_contact(contact_date_from=contact_date_from,
                                                             contact_date_to=contact_date_to,
                                                             region=region)
        if response:
            if offset + len(response[offset:limit + offset]) < len(response):
                has_more = True
            else:
                has_more = False
            formatted = {"items": response[offset:limit + offset],
                         "totalResults": len(response),
                         "limit": limit,
                         "offset": offset,
                         "count": len(response[offset:limit + offset]),
                         "has more": has_more}
            return jsonify(formatted), 200
        else:
            formatted = {"items": response,
                         "totalResults": 0,
                         "limit": limit,
                         "offset": 0,
                         "count": 0,
                         "has more": False}
            return jsonify(formatted), 200
    else:
        return jsonify({
            "failures": [
                {
                    "field": "limit",
                    "value": "1000000",
                    "constraint": "Must be a positive integer value, at most 1000000, if specified."
                }
            ]
        }), 400


@app.route('/person/activity', methods=["POST"])
@auth.login_required()
def import_person_activity():
    data = request.get_json(force=True)
    checked_data = check_data_schema(data=data, fields=person_activity_fields)
    if checked_data:
        db = get_db()
        output = db.insert_data(collection="personActivity", data=checked_data)
        if output:
            return jsonify({"success": True}), 201
    else:
        return "The service has encountered an error. Please make sure the data sent has correct fields. \n" \
               "Accepted fields are: \n " \
               "For testing purposes, the service currently only accepts data from ES or UK or DE." \
               "{0}".format(person_activity_fields), 500


@app.route('/institution', methods=["POST"])
@auth.login_required()
def import_institution():
    data = request.get_json(force=True)
    checked_data = check_data_schema(data=data, fields=institution_fields)
    if checked_data:
        db = get_db()
        output = db.insert_data(collection="institution", data=checked_data)
        if output:
            return jsonify({"success": True}), 201
    else:
        return "The service has encountered an error. Please make sure the data sent has correct fields. \n" \
               "Accepted fields are: \n " \
               "For testing purposes, the service currently only accepts data from ES or UK or DE." \
               "{0}".format(institution_fields), 500


@app.route('/person/institution', methods=["POST"])
@auth.login_required()
def import_person_institution():
    data = request.get_json(force=True)
    checked_data = check_data_schema(data=data, fields=person_institution_fields)
    if checked_data:
        db = get_db()
        output = db.insert_data(collection="personInstitution", data=checked_data)
        if output:
            return jsonify({"success": True}), 201
    else:
        return "The service has encountered an error. Please make sure the data sent has correct fields. \n" \
               "Accepted fields are: \n " \
               "For testing purposes, the service currently only accepts data from ES or UK or DE." \
               "{0}".format(person_institution_fields), 500


@app.route('/contact', methods=["GET", "POST"])
@auth.login_required()
def contact():
    if request.method == "POST":
        data = request.get_json(force=True)
        checked_data = check_data_schema(data=data, fields=contact_fields)
        if checked_data:
            db = get_db()
            output = db.insert_data(collection="contact", data=checked_data)
            if output:
                return jsonify({"success": True}), 201
        # to be edited
        return "The service has encountered an error. \n" \
               "One problem might be all data being sent contains invalid field names, \n" \
               "or all fields 'C_IM_CRM_Contact_ID1' and 'C_IM_CRM_Security_Label1' contain empty values. \n" \
               "For testing purposes, the service currently only accepts data from ES or UK or DE. " \
               "Accepted fields are: \n" \
               "{0}".format(contact_fields), 500
    elif request.method == "GET":
        date_from = request.args.get("dateFrom", "")
        date_to = request.args.get("dateTo", "")
        region = request.args.get("label", "")
        limit = int(request.args.get("limit", 5000))
        offset = int(request.args.get("offset", 0))
        # label is specified
        if region:
            if limit <= 5000:
                response = client.export_contact(date_from=date_from,
                                                 date_to=date_to,
                                                 region=region)
                if response:
                    if offset + len(response[offset:limit + offset]) < len(response):
                        has_more = True
                    else:
                        has_more = False
                    formatted = {"items": response[offset:limit + offset],
                                 "totalResults": len(response),
                                 "limit": limit,
                                 "offset": offset,
                                 "count": len(response[offset:limit + offset]),
                                 "has more": has_more}
                    return jsonify(formatted), 200
                else:
                    formatted = {"items": response,
                                 "totalResults": 0,
                                 "limit": limit,
                                 "offset": 0,
                                 "count": 0,
                                 "has more": False}
                    return jsonify(formatted), 200
            else:
                return jsonify({
                    "failures": [
                        {
                            "field": "limit",
                            "value": "1000000",
                            "constraint": "Must be a positive integer value, at most 1000000, if specified."
                        }
                    ]
                }), 400
        else:
            return "Parameter 'label' in url cannot be empty.", 500
    else:
        return "Method not allowed", 404


@auth.verify_password
def verify(username, password):
    if username == settings.DB_USER and password == settings.DB_PASSWORD:
        return True
    else:
        return False


if __name__ == '__main__':
    app.run()
