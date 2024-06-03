from flask import Flask, request
from db import get_db_connection, close_db_connection
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import json
from datetime import datetime

app = Flask(__name__)

@app.route("/identity", methods=["POST"])
def identity_handler():
	if request.method == "POST":
		# get the data posted in the HTTP POST request
		post_data = request.get_json()
		email_inpt = post_data["email"]
		phone_number_inpt = post_data["phoneNumber"]
		# prepare the consolidated response
		consolidated_resp = {
			"contact": {
				"primaryContactId": None,
				"emails": [],
				"phoneNumbers": [],
				"secondaryContactIds": []
			}
		}

		inpt_email_not_present_flag = False
		inpt_phone_number_not_present_flag = False

		# process of converting primary records into secondary, if required
		if email_inpt is not None and phone_number_inpt is not None:
			conn = get_db_connection()
			try:
				# find the primary record ids associated with the email and phone number
				email_primary_recd_id = None
				phone_number_primary_recd_id = None
				get_email_primary_recd_id_query = "SELECT * FROM contact WHERE email = ('%s') LIMIT 1;" % (email_inpt)
				get_phone_number_primary_recd_id_query = "SELECT * FROM contact WHERE phone_number = ('%s') LIMIT 1;" % (phone_number_inpt)
				get_email_primary_recd_id_resp =  conn.execute(text(get_email_primary_recd_id_query))
				conn.commit()
				get_phone_number_primary_recd_id_resp =  conn.execute(text(get_phone_number_primary_recd_id_query))
				conn.commit()
				email_recd = get_email_primary_recd_id_resp.fetchone()
				phone_number_recd = get_phone_number_primary_recd_id_resp.fetchone()
				if email_recd is not None and phone_number_recd is not None:
					email_recd_link_prcd = email_recd[4]
					phone_number_link_prcd = phone_number_recd[4]
					if email_recd_link_prcd == "primary":
						email_primary_recd_id = email_recd[0]
					elif email_recd_link_prcd == "secondary":
						email_primary_recd_id = email_recd[3]
					if phone_number_link_prcd == "primary":
						phone_number_primary_recd_id = phone_number_recd[0]
					elif phone_number_link_prcd == "secondary":
						phone_number_primary_recd_id = phone_number_recd[3]
				
				# check if both the primary ids are same or not
				if email_primary_recd_id != phone_number_primary_recd_id:
					# find the counts of second. records associated with email & phone_number
					find_secondary_recd_count_email_query = "SELECT COUNT(*) FROM contact WHERE linked_id = (%s);" % (email_primary_recd_id)
					count_resp = conn.execute(text(find_secondary_recd_count_email_query))
					conn.commit()
					email_sec_recd_cnt = count_resp.fetchone()
					find_secondary_recd_count_phone_number_query = "SELECT COUNT(*) FROM contact WHERE linked_id = (%s);" % (phone_number_primary_recd_id)
					count_resp = conn.execute(text(find_secondary_recd_count_phone_number_query))
					conn.commit()
					phone_number_sec_recd_cnt = count_resp.fetchone()
					# do processing for smaller number of records			
					if email_sec_recd_cnt <= phone_number_sec_recd_cnt:
						# make all secondary records point to new primary record by updating linked_id
						update_secnd_recd_query = "UPDATE contact SET linked_id = (%s) WHERE linked_id = (%s);" % (phone_number_primary_recd_id, email_primary_recd_id)
						conn.execute(text(update_secnd_recd_query))
						conn.commit()
						# convert primary_record to secondary_record
						convert_primary_recd_query = "UPDATE contact SET linked_id = (%s), link_precedence = 'secondary' WHERE id = (%s)" % (phone_number_primary_recd_id, email_primary_recd_id)
						conn.execute(text(convert_primary_recd_query))
						conn.commit()	
					else:
						# make all secondary records point to new primary record by updating linked_id
						update_secnd_recd_query = "UPDATE contact SET linked_id = (%s) WHERE linked_id = (%s);" % (email_primary_recd_id, phone_number_primary_recd_id)
						conn.execute(text(update_secnd_recd_query))
						conn.commit()
						# convert primary_record to secondary_record
						convert_primary_recd_query = "UPDATE contact SET linked_id = (%s), link_precedence = 'secondary' WHERE id = (%s)" % (email_primary_recd_id, phone_number_primary_recd_id)
						conn.execute(text(convert_primary_recd_query))
						conn.commit()
			except SQLAlchemyError as e:
				print(f"SQLAlchemyError: {e}")
			close_db_connection()

		# process email input
		if email_inpt is not None:
			conn = get_db_connection()
			query = "SELECT * FROM contact WHERE email = ('%s');" % (email_inpt)
			try:
				resp = conn.execute(text(query))
				conn.commit()
				resp_rows = resp.fetchall()
				if len(resp_rows) != 0:
					for row in resp_rows:
						contact_id = row[0]
						phone_number = row[1]
						email = row[2]
						linked_id = row[3]
						link_prcd = row[4]
						if link_prcd == "primary":
							consolidated_resp["contact"]["primaryContactId"] = contact_id
							# process all records with linked_id as the contact_id of the current record
							rows_with_same_linked_id_query = "SELECT * FROM contact WHERE linked_id = (%s);" % (contact_id)
							rows_with_same_linked_id_resp = conn.execute(text(rows_with_same_linked_id_query))
							conn.commit()
							rows_with_same_linked_id_resp_records = rows_with_same_linked_id_resp.fetchall()
							for recd in rows_with_same_linked_id_resp_records:
								recd_contact_id = recd[0]
								recd_phone_number = recd[1]
								recd_email = recd[2]
								if recd_phone_number is not None and recd_phone_number not in consolidated_resp["contact"]["phoneNumbers"]: # check if phone_number is not already present
									consolidated_resp["contact"]["phoneNumbers"].append(recd_phone_number) # add to resp.
								if recd_email is not None and recd_email not in consolidated_resp["contact"]["emails"]: # check if email is not already present
									consolidated_resp["contact"]["emails"].append(recd_email) # add to resp.
								if recd_contact_id not in consolidated_resp["contact"]["secondaryContactIds"]:
									consolidated_resp["contact"]["secondaryContactIds"].append(recd_contact_id)
						elif link_prcd == "secondary":
							if contact_id not in consolidated_resp["contact"]["secondaryContactIds"]:
								consolidated_resp["contact"]["secondaryContactIds"].append(contact_id)
							if consolidated_resp["contact"]["primaryContactId"] == None:
								consolidated_resp["contact"]["primaryContactId"] = linked_id
								get_primary_recd_query = "SELECT * FROM contact WHERE id = (%s);" % (linked_id)
								get_primary_recd_resp = conn.execute(text(get_primary_recd_query))
								conn.commit()
								get_primary_recd_resp_row = get_primary_recd_resp.fetchone()
								pr_phone_number = get_primary_recd_resp_row[1]
								pr_email = get_primary_recd_resp_row[2]
								if pr_email is not None and pr_email not in consolidated_resp["contact"]["emails"]: # check if email is not already present
									consolidated_resp["contact"]["emails"].append(pr_email) # add to resp.
								if pr_phone_number is not None and pr_phone_number not in consolidated_resp["contact"]["phoneNumbers"]: # check if phone_number is not already present
									consolidated_resp["contact"]["phoneNumbers"].append(pr_phone_number) # add to resp.
							# process all records with same linked_id as current record
							rows_with_same_linked_id_query = "SELECT * FROM contact WHERE linked_id = (%s);" % (linked_id)
							rows_with_same_linked_id_resp = conn.execute(text(rows_with_same_linked_id_query))
							conn.commit()
							rows_with_same_linked_id_resp_records = rows_with_same_linked_id_resp.fetchall()
							for recd in rows_with_same_linked_id_resp_records:
								recd_contact_id = recd[0]
								recd_phone_number = recd[1]
								recd_email = recd[2]
								if recd_phone_number is not None and recd_phone_number not in consolidated_resp["contact"]["phoneNumbers"]: # check if phone_number is not already present
									consolidated_resp["contact"]["phoneNumbers"].append(recd_phone_number) # add to resp.
								if recd_email is not None and recd_email not in consolidated_resp["contact"]["emails"]: # check if email is not already present
									consolidated_resp["contact"]["emails"].append(recd_email) # add to resp.
								if recd_contact_id not in consolidated_resp["contact"]["secondaryContactIds"]:
									consolidated_resp["contact"]["secondaryContactIds"].append(recd_contact_id)
						if email not in consolidated_resp["contact"]["emails"]: # check if email is not already present
							consolidated_resp["contact"]["emails"].append(email) # add to resp.
						if phone_number is not None and phone_number not in consolidated_resp["contact"]["phoneNumbers"]: # check if phone_number is not already present
							consolidated_resp["contact"]["phoneNumbers"].append(phone_number) # add to resp.
				else:
					# the email recv. in request is not present in the database
					inpt_email_not_present_flag = True
			except SQLAlchemyError as e:
				print(f"SQLAlchemyError: {e}")
			close_db_connection()

		# process the phone number input
		if phone_number_inpt is not None:
			conn = get_db_connection()
			query = "SELECT * FROM contact WHERE phone_number = ('%s');" % (phone_number_inpt)
			try:
				resp = conn.execute(text(query))
				conn.commit()
				resp_rows = resp.fetchall()
				if len(resp_rows) != 0:
					for row in resp_rows:
						contact_id = row[0]
						phone_number = row[1]
						email = row[2]
						linked_id = row[3]
						link_prcd = row[4]
						if link_prcd == "primary":
							consolidated_resp["contact"]["primaryContactId"] = contact_id
							# process all records with linked_id as the contact_id of the current record
							rows_with_same_linked_id_query = "SELECT * FROM contact WHERE linked_id = (%s);" % (contact_id)
							rows_with_same_linked_id_resp = conn.execute(text(rows_with_same_linked_id_query))
							conn.commit()
							rows_with_same_linked_id_resp_records = rows_with_same_linked_id_resp.fetchall()
							for recd in rows_with_same_linked_id_resp_records:
								recd_contact_id = recd[0]
								recd_phone_number = recd[1]
								recd_email = recd[2]
								if recd_email is not None and recd_email not in consolidated_resp["contact"]["emails"]: # check if email is not already present
									consolidated_resp["contact"]["emails"].append(recd_email) # add to resp.
								if recd_phone_number is not None and recd_phone_number not in consolidated_resp["contact"]["phoneNumbers"]: # check if phone_number is not already present
									consolidated_resp["contact"]["phoneNumbers"].append(recd_phone_number) # add to resp.
								if recd_contact_id not in consolidated_resp["contact"]["secondaryContactIds"]:
									consolidated_resp["contact"]["secondaryContactIds"].append(recd_contact_id)
						elif link_prcd == "secondary":
							if contact_id not in consolidated_resp["contact"]["secondaryContactIds"]:
								consolidated_resp["contact"]["secondaryContactIds"].append(contact_id)
							if consolidated_resp["contact"]["primaryContactId"] == None:
								consolidated_resp["contact"]["primaryContactId"] = linked_id
								get_primary_recd_query = "SELECT * FROM contact WHERE id = (%s);" % (linked_id)
								get_primary_recd_resp = conn.execute(text(get_primary_recd_query))
								conn.commit()
								get_primary_recd_resp_row = get_primary_recd_resp.fetchone()
								pr_phone_number = get_primary_recd_resp_row[1]
								pr_email = get_primary_recd_resp_row[2]
								if pr_email is not None and pr_email not in consolidated_resp["contact"]["emails"]: # check if email is not already present
									consolidated_resp["contact"]["emails"].append(pr_email) # add to resp.
								if pr_phone_number is not None and pr_phone_number not in consolidated_resp["contact"]["phoneNumbers"]: # check if phone_number is not already present
									consolidated_resp["contact"]["phoneNumbers"].append(pr_phone_number) # add to resp.
							# process all records with same linked_id as current record
							rows_with_same_linked_id_query = "SELECT * FROM contact WHERE linked_id = (%s);" % (linked_id)
							rows_with_same_linked_id_resp = conn.execute(text(rows_with_same_linked_id_query))
							conn.commit()
							rows_with_same_linked_id_resp_records = rows_with_same_linked_id_resp.fetchall()
							for recd in rows_with_same_linked_id_resp_records:
								recd_contact_id = recd[0]
								recd_phone_number = recd[1]
								recd_email = recd[2]
								if recd_email is not None and recd_email not in consolidated_resp["contact"]["emails"]: # check if email is not already present
									consolidated_resp["contact"]["emails"].append(recd_email) # add to resp.
								if recd_phone_number is not None and recd_phone_number not in consolidated_resp["contact"]["phoneNumbers"]: # check if phone_number is not already present
									consolidated_resp["contact"]["phoneNumbers"].append(recd_phone_number) # add to resp.
								if recd_contact_id not in consolidated_resp["contact"]["secondaryContactIds"]:
									consolidated_resp["contact"]["secondaryContactIds"].append(recd_contact_id)
						if phone_number not in consolidated_resp["contact"]["phoneNumbers"]: # check if phone_number is not already present
							consolidated_resp["contact"]["phoneNumbers"].append(phone_number) # add to resp.
						if email is not None and email not in consolidated_resp["contact"]["emails"]: # check if email is not already present
							consolidated_resp["contact"]["emails"].append(email) # add to resp.
				else:
					# the phone_number recv. in request is not present in the database
					inpt_phone_number_not_present_flag = True
			except SQLAlchemyError as e:
				print(f"SQLAlchemyError: {e}")
			close_db_connection()

		# both email and phone_number not present in the database, create a new primary record
		if inpt_email_not_present_flag == True and inpt_phone_number_not_present_flag == True:
			# insert new primary record
			conn = get_db_connection()
			insert_query = "INSERT INTO contact(phone_number, email, linked_id, link_precedence, created_at, updated_at, deleted_at) VALUES(('%s'), ('%s'), null, 'primary', ('%s'), ('%s'), null);" % (phone_number_inpt, email_inpt, datetime.now(), datetime.now())
			try:
				conn.execute(text(insert_query))
				conn.commit()
				get_newly_inserted_recd_query = "SELECT * FROM contact WHERE email = ('%s');" % (email_inpt)
				resp = conn.execute(text(get_newly_inserted_recd_query))
				conn.commit()
				resp_row = resp.fetchone()
				consolidated_resp["contact"]["primaryContactId"] = resp_row[0]
				consolidated_resp["contact"]["phoneNumbers"].append(resp_row[1])
				consolidated_resp["contact"]["emails"].append(resp_row[2])
			except SQLAlchemyError as e:
				print(f"SQLAlchemyError: {e}")
			close_db_connection()

		# email is unseen and phone_number is null, create a new primary record
		if phone_number_inpt == None and inpt_email_not_present_flag == True:
			# insert new primary record
			conn = get_db_connection()
			insert_query = "INSERT INTO contact(phone_number, email, linked_id, link_precedence, created_at, updated_at, deleted_at) VALUES(null, ('%s'), null, 'primary', ('%s'), ('%s'), null);" % (email_inpt, datetime.now(), datetime.now())
			try:
				conn.execute(text(insert_query))
				conn.commit()
				get_newly_inserted_recd_query = "SELECT * FROM contact WHERE email = ('%s');" % (email_inpt)
				resp = conn.execute(text(get_newly_inserted_recd_query))
				conn.commit()
				resp_row = resp.fetchone()
				consolidated_resp["contact"]["primaryContactId"] = resp_row[0]
				consolidated_resp["contact"]["emails"].append(resp_row[2])
			except SQLAlchemyError as e:
				print(f"SQLAlchemyError: {e}")
			close_db_connection()

		# phone_number is unseen and email is null, create a new primary record 
		if email_inpt == None and inpt_phone_number_not_present_flag == True:
 			# insert new primary record
			conn = get_db_connection()
			insert_query = "INSERT INTO contact(phone_number, email, linked_id, link_precedence, created_at, updated_at, deleted_at) VALUES(('%s'), null, null, 'primary', ('%s'), ('%s'), null);" % (phone_number_inpt, datetime.now(), datetime.now())
			try:
				conn.execute(text(insert_query))
				conn.commit()
				get_newly_inserted_recd_query = "SELECT * FROM contact WHERE phone_number = ('%s');" % (phone_number_inpt)
				resp = conn.execute(text(get_newly_inserted_recd_query))
				conn.commit()
				resp_row = resp.fetchone()
				consolidated_resp["contact"]["primaryContactId"] = resp_row[0]
				consolidated_resp["contact"]["phoneNumbers"].append(resp_row[1])
			except SQLAlchemyError as e:
				print(f"SQLAlchemyError: {e}")
			close_db_connection()

 		# only email is unseen, create a secondary record & update consolidated record
		if inpt_email_not_present_flag == True and inpt_phone_number_not_present_flag == False and phone_number_inpt is not None:
			# insert a new secondary record
			conn = get_db_connection()
			try:
				linked_id = consolidated_resp["contact"]["primaryContactId"]
				insert_query = "INSERT INTO contact(phone_number, email, linked_id, link_precedence, created_at, updated_at, deleted_at) VALUES(('%s'), ('%s'), (%s), 'secondary', ('%s'), ('%s'), null);" % (phone_number_inpt, email_inpt, linked_id, datetime.now(), datetime.now())
				conn.execute(text(insert_query))
				conn.commit()
				get_newly_inserted_recd_query = "SELECT * FROM contact WHERE email = ('%s');" % (email_inpt)
				resp = conn.execute(text(get_newly_inserted_recd_query))
				conn.commit()
				resp_row = resp.fetchone()
				if consolidated_resp["contact"]["primaryContactId"] == None:
					consolidated_resp["contact"]["primaryContactId"] = linked_id
				consolidated_resp["contact"]["emails"].append(email_inpt)
				consolidated_resp["contact"]["secondaryContactIds"].append(resp_row[0])
			except SQLAlchemyError as e:
				print(f"SQLAlchemyError: {e}")
			close_db_connection()

		# only phone_number is unseen, create a secondary record
		if inpt_phone_number_not_present_flag == True and inpt_email_not_present_flag == False and email_inpt is not None:
		# insert a new secondary record
			conn = get_db_connection()
			try:
				linked_id = consolidated_resp["contact"]["primaryContactId"]
				insert_query = "INSERT INTO contact(phone_number, email, linked_id, link_precedence, created_at, updated_at, deleted_at) VALUES(('%s'), ('%s'), (%s), 'secondary', ('%s'), ('%s'), null);" % (phone_number_inpt, email_inpt, linked_id, datetime.now(), datetime.now())
				conn.execute(text(insert_query))
				conn.commit()
				get_newly_inserted_recd_query = "SELECT * FROM contact WHERE phone_number = ('%s');" % (phone_number_inpt)
				resp = conn.execute(text(get_newly_inserted_recd_query))
				conn.commit()
				resp_row = resp.fetchone()
				if consolidated_resp["contact"]["primaryContactId"] == None:
					consolidated_resp["contact"]["primaryContactId"] = linked_id
				consolidated_resp["contact"]["phoneNumbers"].append(phone_number_inpt)
				consolidated_resp["contact"]["secondaryContactIds"].append(resp_row[0])
			except SQLAlchemyError as e:
				print(f"SQLAlchemyError: {e}")
			close_db_connection()
		
		consolidated_resp_json = json.dumps(consolidated_resp, indent=4)
		return consolidated_resp_json