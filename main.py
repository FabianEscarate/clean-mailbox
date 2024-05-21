import email.utils
import os
from sys import argv
import imaplib
import email
import csv
from itertools import groupby
import sqlite3

def chunk_array(array, size):
    chunked = []
    for i in range(0, len(array), size):
        chunked.append(array[i:i + size])
    return chunked

def db_get_connection():
  return sqlite3.connect(os.environ['SQLCONNECTIONSTRING'])

def db_create_database():
  try:
    with db_get_connection() as conn:    
      cursor = conn.cursor()
      cursor.execute('create table domains (mail_id, domain)')
      conn.commit()
  except Exception as e:
    print(f"some error {e}")
  finally:
    conn.close()
    
def db_clean_database():
  try:
    with db_get_connection() as conn:
      cursor = conn.cursor()
      cursor.executescript('delete from domains')
      conn.commit()
  except Exception as e:
    print(f"some error {e}")
  finally:
    conn.close()

def db_insert_domain(msg_id, domain):
  try:
    with db_get_connection() as conn:
      cursor = conn.cursor()
      cursor.execute('insert into domains values (?, ?)',(msg_id, domain))
      conn.commit()
  except Exception as e:
    print(f"some error {e}")
  finally:
    conn.close()

def imap_connect_to_mailbox(mailbox:str = 'inbox', search:str = 'ALL'):
  # Account credentials
  username = os.environ['EMAIL']
  password = os.environ['PASSWORD']

  # Connect to the Outlook IMAP server
  mail = imaplib.IMAP4_SSL("outlook.office365.com")
  # Log in to your account
  mail.login(username, password)
  # Select the mailbox you want to use (e.g., inbox)
  mail.select(mailbox)
  # Search for all emails in the mailbox
  status, messages = mail.search(None, search)
  return (mail, status, messages)

def imap_create_mailboxs(folders_to_create: list[str]):
  domains_to_filter = folders_to_create
  mail, status, messages = imap_connect_to_mailbox()
  for folder_domain in domains_to_filter:
    status, data = mail.create(folder_domain)
    if(status == 'OK'):
      print(f'folder {folder_domain} created')
    if status == 'NO':
      print(f'{data}')

  mail.close()
  mail.logout()
  pass

def imap_reconect(mailbox:str = 'inbox'):
   # Account credentials
  username = os.environ['EMAIL']
  password = os.environ['PASSWORD']

  # Connect to the Outlook IMAP server
  mail = imaplib.IMAP4_SSL("outlook.office365.com")
  # Log in to your account
  mail.login(username, password)
  mail.select(mailbox)
  return mail

def export_to_db():
  db_create_database()
  mail, status, messages = imap_connect_to_mailbox()
  if status == 'OK':
    print('cantidad de mails', len(messages[0].split()))
    messages_ids = messages[0].split()
    for message_id in messages_ids:
        status, msg_data = mail.fetch(message_id, "(RFC822)")
        if status == 'OK':
          msg = email.message_from_bytes(msg_data[0][1])
          msg_from = email.utils.parseaddr(msg.get('from'))[1]
          domain = msg_from.split('@')[-1].lower()
          print(message_id.decode(), domain)
          db_insert_domain(message_id, domain)
  mail.close()
  mail.logout()
  
def export_to_csv():
  file = open('db_domains.csv','w')
  mail, status, messages = imap_connect_to_mailbox()
  if status == 'OK':
    print('cantidad de mails', len(messages[0].split()))
    messages_ids = messages[0].split()
    for message_id in messages_ids:
        status, msg_data = mail.fetch(message_id, "(RFC822)")
        if status == 'OK':
          msg = email.message_from_bytes(msg_data[0][1])
          msg_from = email.utils.parseaddr(msg.get('from'))[1]
          domain = msg_from.split('@')[-1].lower()
          print(message_id.decode(), domain)
          file.write("{message_id},{domain}\n".format(message_id= int(message_id), domain= domain))
  
  file.close()
  mail.close()
  mail.logout()

def generate_report(data_from:str):
  if data_from == 'DB':
    with db_get_connection() as conn:
      with open('domain_report.csv', 'w') as file:
        cursor = conn.cursor()
        writer = csv.writer(file)
        writer.writerow(['domain', 'cantidad'])
        for row in cursor.execute(f"SELECT domain, COUNT(domain) as cantidad from domains d group by domain ORDER by cantidad desc"):
          writer.writerow(row)
    pass
  if data_from == 'CSV':
    Exception('no implemented')
    pass
  pass

def sort_emails():
  domains_to_filter = ['kibernum.com']
  # imap_create_mailboxs(domains_to_filter)
  for domain_to_filter in domains_to_filter:
    mail, status, messages = imap_connect_to_mailbox(search=f'FROM "@{domain_to_filter.lower()}"')
    mail.close()
    mail.logout()

    if status == 'OK':
      print('cantidad de mails', len(messages[0].split()))
      messages_ids = messages[0].split()[::-1]
      chunkMessages = chunk_array(messages_ids, 10)
      for chunk in chunkMessages:
        mail = imap_reconect()
        for message_id in chunk:
            status, msg_data = mail.fetch(message_id, "(RFC822)")
            if status == 'OK':
              msg = email.message_from_bytes(msg_data[0][1])
              msg_from = email.utils.parseaddr(msg.get('from'))[1]
              domain = msg_from.split('@')[-1].lower()
              print(msg_from, domain, domain_to_filter)
              if domain == domain_to_filter.lower():
                try:
                    mail.create(domain_to_filter)
                except imaplib.IMAP4.error:
                  pass  # Folder already exists
                mail.copy(message_id, domain_to_filter)
                mail.store(message_id, '+FLAGS', '\\Deleted')
                print(f'{msg_from} moved')
    
        mail.expunge()
        mail.close()
        mail.logout()

def main():
  mailbox = 'inbox'

  listDomains = list()

  mail, status, messages = imap_connect_to_mailbox()
  n_mensajes = len(messages[0].split())
  print(f'{mailbox} have {n_mensajes} mails')
  for msg_id in messages[0].split():
    from_email = email.utils.parseaddr(email.message_from_bytes(mail.fetch(msg_id, "(RFC822)")[1][0][1]).get('from'))[1]
    domain = from_email.split('@')[-1]
    listDomains.append(domain)
    print(f'reading emails ({msg_id.decode()}/{n_mensajes})', end='\r')
  print('')

  groupedDomains = list(map(lambda k:
                              {
                                "domain":k[0],
                                "cantidad": len(list(k[1]))
                              }
                              , groupby(sorted(listDomains, key=lambda x: x[0]), key=lambda x: x)
                            )
                        )
  
  groupedDomains.sort(key= lambda g: g['cantidad'], reverse=True)
  
  print('')
  for group in groupedDomains:
    print(f'{group["domain"]} : {group["cantidad"]}')
  
  mail.close()
  mail.logout()

def help():
  print(
"""Script to create folders in mailbox, with names of domains more repeated on your inbox

Options:

--help             show this help message
--cleanDatabase    clean table from database
--exportDB         export data domains in a sqlite database
--exportCSV        export data domains in a csv file
""")

if __name__ == "__main__":
  if('--cleanDatabase' in argv):
    db_clean_database()
  if '--help' in argv:
    help()
  if '--exportDB' in argv:
    export_to_db()
  if '--exportCSV' in argv:
    export_to_db()
  if '--sortEmail' in argv:
    sort_emails()
  if '--reportDB' in argv:
    generate_report('DB')
  if len(argv) == 1:
    main()
  # export_to_csv()
  # main()
