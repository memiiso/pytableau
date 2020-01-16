#! /usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os
import shutil
import sys
import time

import pandas as pd
import tableaudocumentapi
import tableauserverclient as TSC

log = logging.getLogger('PyTableau')
log.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)


class PyTableau(object):
    """

    """

    def __init__(self, server_address, username, password, site_id, use_server_version=True, verify_ssl=True):

        self.tableau_auth = TSC.TableauAuth(username=username, password=password, site_id=site_id)
        self.server = TSC.Server(server_address=server_address, use_server_version=use_server_version)

        self.server.add_http_options({'verify': verify_ssl})
        log.info("Tableau Server Address %s " % self.server.server_address)
        log.info("Tableau Server Baseurl %s " % self.server.baseurl)
        self.sign_in()

    def sign_in(self):
        self.tableau_auth = self.server.auth.sign_in(self.tableau_auth)

    def download_all_datasources(self, download_dir, include_extract=False):
        """

        :param download_dir:
        :param include_extract:
        """
        utils.clean_folder(download_dir)
        for server_datasource in TSC.Pager(self.server.datasources):
            ds_download_dir = os.path.join(download_dir, server_datasource.project_name)
            if not os.path.exists(ds_download_dir):
                os.makedirs(ds_download_dir)
            path = self.server.datasources.download(server_datasource.id, filepath=ds_download_dir,
                                                    include_extract=include_extract)
            log.info("Downloaded datasource: %s " % path)
        log.info("Download Completed! Download directory %s" % download_dir)

    def download_all_workbooks(self, download_dir):
        """
        download all workbooks from server to given directory

        :param download_dir:
        """

        utils.clean_folder(download_dir)
        for server_workbook in TSC.Pager(self.server.workbooks):
            wb_download_dir = os.path.join(download_dir, server_workbook.project_name)
            if not os.path.exists(wb_download_dir):
                os.makedirs(wb_download_dir)
            try:
                path = self.server.workbooks.download(server_workbook.id, filepath=wb_download_dir,
                                                      include_extract=False)
                log.info("Downloaded workbook: %s " % path)
            except Exception:
                # pass this error "UnicodeEncodeError 'ascii' codec can't encode characters in position : ordinal
                # not in range(128)"
                log.info("Skipping workbook %s " % path)
        log.info("Download Completed! Download directory %s " % download_dir)

    def get_all_workbook_fields(self, workbooks_dir):
        """
        get all fields from workbooks found in given directory

        :param workbooks_dir:
        :return:
        """
        log.info("Extracting all workbook fields")
        rows_list = []

        for root, _, filenames in os.walk(workbooks_dir):
            for filename in filenames:
                if "twb" in filename:
                    # read metadata of workbook
                    try:
                        log.info("Processing %s " % filename)
                        my_wb = tableaudocumentapi.workbook.Workbook(os.path.join(root, filename))
                    except Exception:
                        continue
                    for myDS in my_wb.datasources:
                        for _, field in myDS.fields.items():
                            if len(field.worksheets) > 0:
                                for myWorksheet in field.worksheets:
                                    field_dict = self._field_dict(field, datasource=myDS, workbook=my_wb,
                                                                  worksheet=myWorksheet)
                                    rows_list.append(field_dict)
                            else:
                                field_dict = self._field_dict(field, datasource=myDS, workbook=my_wb)
                                rows_list.append(field_dict)
        return pd.DataFrame(rows_list)

    def get_all_datasource_fields(self, datasource_dir):
        """

        :param datasource_dir:
        :return:
        """
        rows_list = []
        for root, _, filenames in os.walk(datasource_dir):
            for filename in filenames:
                if "tds" in filename:
                    log.info("Processing " + filename)
                    # read metadata of workbook
                    my_ds = tableaudocumentapi.datasource.Datasource.from_file(os.path.join(root, filename))
                    for _, field in my_ds.fields.items():
                        field_dict = self._field_dict(field, datasource=my_ds)
                        rows_list.append(field_dict)

        return pd.DataFrame(rows_list)

    def export_all_workbook_fields_to_csv(self, workbooks_dir):
        """

        :param workbooks_dir:
        """
        self.download_all_workbooks(download_dir=workbooks_dir)
        field_list_file = workbooks_dir + '/all_workbook_fields.csv'
        log.info("Putting all workbook fields into %s " % field_list_file)
        df_wb_fields = self.get_all_workbook_fields(workbooks_dir)
        df_wb_fields.to_csv(field_list_file, sep='\t', encoding='utf-8')
        log.info("Created %s " % field_list_file)

    def export_all_datasource_fields_to_csv(self, datasource_dir):
        """

        :param datasource_dir:
        """
        self.download_all_workbooks(download_dir=datasource_dir)
        field_list_file = datasource_dir + '/all_datasource_fields.csv'
        log.info("Putting all datasource fields into %s " % (field_list_file,))
        df_wb_fields = self.get_all_datasource_fields(datasource_dir)
        df_wb_fields.to_csv(field_list_file, sep='\t', encoding='utf-8')
        log.info("Created %s" % field_list_file)

    def _field_dict(self, field, datasource, workbook=None, worksheet=None):
        """

        :param field:
        :param datasource:
        :param workbook:
        :param worksheet:
        :return:
        """
        row = {"field_name": utils.NoneToStr(field.caption), "field_aggregation": utils.NoneToStr(field._aggregation),
               "field_alias": utils.NoneToStr(field.alias), "field_calculation": utils.NoneToStr(field.calculation),
               "field_datatype": utils.NoneToStr(field.datatype),
               "field_description": utils.NoneToStr(field.description), "field_id": utils.NoneToStr(field.id),
               "field_role": utils.NoneToStr(field.role), "field_type": utils.NoneToStr(field._type),
               'data_source_name': utils.NoneToStr(datasource.name or datasource.caption),
               'data_source_caption': utils.NoneToStr(datasource.caption),
               'data_source_version': utils.NoneToStr(datasource.version), 'data_source_connections': '@TODO'}

        if workbook:
            row['workbook_name'] = utils.NoneToStr(os.path.basename(workbook.filename))
        else:
            row['workbook_name'] = ''

        if worksheet:
            row['worksheet_name'] = utils.NoneToStr(worksheet)
        else:
            row['workbook_name'] = ''

        return row

    def refresh_extracts(self, datasource_names, retry_attempt=2, synchronous=False):
        """

        :param datasource_names:
        :param retry_attempt:
        """
        log.info("Refreshing Datasources %s on %s " % (str(datasource_names), self.server.server_address))

        datasource_names = [item.lower() for item in datasource_names]
        datasource_list_immutable_copy = datasource_names.copy()
        datasource_list_server = dict()
        extract_refresh_jobs = dict()

        # get all datasources from server
        for ds in TSC.Pager(self.server.datasources):
            datasource_list_server[ds.id] = ds

        # loop over server datasources and refresh if its name found in given DS list
        for ds in datasource_list_server.values():
            try:
                if ds.name.lower() in datasource_list_immutable_copy:
                    log.info('Refreshing Datasource "%s" ' % ds.name)
                    refresh_job = self.refresh_extract(ds_item=ds, attempt=retry_attempt)
                    if refresh_job:
                        extract_refresh_jobs[ds.name + ':' + refresh_job.id] = refresh_job
                    datasource_names.remove(ds.name.lower())
            except Exception as e:
                log.warning(str(e).strip())

        if len(datasource_names):
            log.error("Following Datasources are not Refreshed! %s " % str(datasource_names))

        if synchronous is True and len(extract_refresh_jobs) > 0:
            extract_refresh_jobs_immutable_copy = extract_refresh_jobs.copy()
            log.info("Waiting for Extract Jobs to Finish!")
            while len(extract_refresh_jobs) > 0:
                time.sleep(300)
                for key, _job in extract_refresh_jobs_immutable_copy.items():
                    if key in extract_refresh_jobs.keys():  # if its not yet finished!
                        time.sleep(5)
                        job = self.server.jobs.get_by_id(_job.id)
                        log.info("%s Refresh Running %s " % (key, str(job)))
                        if job.completed_at is not None:
                            log.info("%s Refresh Finished in %s " % (key, (job.completed_at - job.started_at)))
                            extract_refresh_jobs.pop(key)

    def refresh_extract(self, ds_item, attempt=1, current_attempt=1):
        """

        :param ds_item:
        :param attempt:
        :param current_attempt:
        :return:
        """
        try:
            results = self.server.datasources.refresh(ds_item)
            return results
        except Exception as e:
            if current_attempt >= attempt:
                raise e
            else:
                current_attempt = current_attempt + 1
                time.sleep(5)
                log.debug(str(e))
                log.info("Refreshing '%s' failed Trying %s th time" % (ds_item.name, str(current_attempt)))
                return self.refresh_extract(ds_item=ds_item, attempt=attempt, current_attempt=current_attempt)


class utils(object):

    @staticmethod
    def merge_two_dicts(x, y):
        """

        :param x:
        :param y:
        :return:
        """
        z = x.copy()  # start with x's keys and values
        z.update(y)  # modifies z with y's keys and values & returns None
        return z

    @staticmethod
    def NoneToStr(string):
        """

        :param string:
        :return:
        """
        return (string or '').replace('\r\n', ' ').replace('\n', ' ').replace('\t', ' ')

    @staticmethod
    def clean_folder(dirPath):
        """

        :param dirPath:
        """
        fileList = os.listdir(dirPath)
        for fileName in fileList:
            file = dirPath + "/" + fileName
            if os.path.isdir(file):
                shutil.rmtree(file)
            else:
                os.remove(file)

            print(file)

    @staticmethod
    def extract_file_name(file):
        """

        :param file:
        :return:
        """
        base = os.path.basename(file)
        return os.path.splitext(base)[0]
