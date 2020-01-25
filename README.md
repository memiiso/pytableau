# PyTableau

tableau class to interact with tableau server and server content

- download_all_datasources
- download_all_workbooks
- export_all_datasource_fields_to_csv
- refresh_extracts
- export_all_workbook_fields_to_csv
- download_workbook_pdf

# PyTableauReportScheduler
tableau class to emailing reports as pdf using workbook tags

forexample a workbook with following tags 

- sent to user1, user2 every day,
- sent to  user3 Weekly on Mondays,
- sent to user4, user2 at the first day of month every month.

```
'scheduledReport'
,'Daily:to:user1@mail.com','Daily:cc:user2@mail.com'
,'Weekly1:to:user3@mail.com'
,'Monthly1:to:user4@mail.com','Monthly1:cc:user2@mail.com'
```

a workbook with following tags sent
- Weekly every Wednesday
- monthly every 16th day of month

```
'scheduledReport'
,'Weekly3:to:user3@mail.com'
,'Monthly16:to:user4@mail.com','Monthly1:cc:user2@mail.com'
```

#### Schedule tags
Daily = send every day

Weekly* = * is weekday number(1...7) sends the report at given weekday

Monthly* = * is month day number(1...31) sends the report at given month day

## PyTableau Examples

#### Init 

```python
import os
import smtplib
from pathlib import Path
from pytableau import PyTableau, PyTableauReportScheduler
from urllib.parse import quote_plus

myTableau = PyTableau(server_address='http_tableau_server',
      username='mytableauuser@mail.com',
      password='mypassword',
      site_id='Default'
      )
```

## Refreshing Datasources
```python
datasource_list = ['my Datasource1', 'my Datasource2', "Datasource3"]
myTableau.refresh_extracts(datasource_names=datasource_list, synchronous=True)
```

## Downloading All Workbooks, Datasources From Server
```python
download_dir = "/tmp/test_download_all_datasources"
Path(download_dir).mkdir(parents=True, exist_ok=True)
myTableau.download_all_workbooks(download_dir)
```

## Querying Workbooks, Datasources From Server
```python
tag='dailyKpi'
wbs  = myTableau.get_workbooks_by_tag(tag=tag)
print("Found %s Workbook with tag:%s" % (str(len(wbs)), tag))
wb  = myTableau.get_workbook_by_name(name='XYZ KPI REPORT', project_name='FINANCETEAM', tag='XYZKPI')
print(wb.name)
print(wb.updated_at)
wb  = myTableau.get_workbook_by_name(name='XYZ REPORT', project_name='XYZ TEAM')
print(wb.name)
```

## Exporting Workbook to PDF
```python
# find the workbook
wb  = myTableau.get_workbook_by_name(name='XYZ DASHBOARD', project_name='PROJECT_NAME')
# export it to PDF
myTableau.download_workbook_pdf(workbook=wb, dest_dir="/tmp/somedir/")
```


## PyTableauReportScheduler Examples

#### Init 
```python
import os
import smtplib
from pathlib import Path
from pytableau import PyTableau, PyTableauReportScheduler
from urllib.parse import quote_plus

myTableau = PyTableau(server_address='http_tableau_server',
      username='mytableauuser@mail.com',
      password='mypassword',
      site_id='Default'
      )

mysmtp = smtplib.SMTP_SSL('smtp.gmail.com', 465)
mysmtp.ehlo()
mysmtp.login('myemail@mail.com', 'mypassword')

# email all the reports with tag : scheduledReport
myTabScheduler= PyTableauReportScheduler(tableau=myTableau,smtp_server=mysmtp,schedule_tag="scheduledReport")
```


## Triggering Reports
```python
datafilters = dict()
datafilters["Report Year"]="2019,2020"
datafilters["Country"]="US"
# send reports daily, weekly, monthly.
myTabScheduler.send_scheduled_reports(send_from='senderemail@mail.com', data_filters=datafilters)

# send weekly Monday reports.
myTabScheduler.send_schedule(send_from='senderemail@mail.com', schedule='Wekkly1',data_filters=datafilters)
```

