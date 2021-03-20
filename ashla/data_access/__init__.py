from astroquery.gaia import GaiaClass
from ashla import utils
import ashla.data_access.config as cnf


class GaiaDataAccess(GaiaClass):
    def __init__(self, login_config=None):
        super(GaiaDataAccess, self).__init__()
        self.login_config = login_config
        if self.login_config is None:
            # You can type in your details
            self.login()
        else:
            login_conf_inst = cnf.GaiaLoginConf(config_file=self.login_config)
            self.login(user=login_conf_inst.user, password=login_conf_inst.password)

    def get_gaia_job(self, query, asyncronous=True, **kwargs):
        if asyncronous:
            j1 = self.launch_job_async(query, **kwargs)
        else:
            j1 = self.launch_job(query, **kwargs)
        job = j1.get_results()
        return job

    def gaia_query_to_pandas(self, query, parquet_output_name=None, **kwargs):
        job = self.get_gaia_job(query, **kwargs)
        gaia_data = job.to_pandas()
        if parquet_output_name is not None:
            self.data_save_parquet(gaia_data, output_file_name=parquet_output_name)
        return gaia_data

    def data_save_parquet(self, data, output_file_name):
        data.to_parquet("{0}.parquet.gzip".format(output_file_name), compression='gzip')

    def gaia_query_save_parquet_file(self, query, output_file_name):
        gaia_data = query_gaia_to_pandas(query)
        self.data_save_parquet(gaia_data, output_file_name)

    def gaia_get_dr2_initial_data(self, save_to_parquet=False, **kwargs):
        """

        Function to run a query showing the first wide binary pair in the paper.

        Args:
            save_to_parquet (bool): Save results to Parquet format.

        Kwargs:
            output_file (str): optional, default None
                file name where the results are saved if dumpToFile is True.
                If this parameter is not provided, the jobid is used instead
            output_format (str): optional, default 'votable'
                results format
            verbose (bool): optional, default 'False'
                flag to display information about the process
            dump_to_file (bool): optional, default 'False'
                if True, the results are saved in a file instead of using memory
            background (bool): optional, default 'False'
                when the job is executed in asynchronous mode, this flag specifies
                whether the execution will wait until results are available
            upload_resource (str): optional, default None
                resource to be uploaded to UPLOAD_SCHEMA
            upload_table_name (str): optional, default None
                resource temporary table name associated to the uploaded resource.
                This argument is required if upload_resource is provided.
            autorun (boolean): optional, default True
                if 'True', sets 'phase' parameter to 'RUN',
                so the framework can start the job.

        Returns:
            Pandas.DataFrame: Output of the query.

        """
        query = r"""SELECT TOP 500 gaia_source.source_id,gaia_source.ra,gaia_source.ra_error,gaia_source.dec,
                        gaia_source.dec_error,gaia_source.parallax,gaia_source.parallax_error,gaia_source.phot_g_mean_mag,
                        gaia_source.bp_rp,gaia_source.radial_velocity,gaia_source.radial_velocity_error,
                        gaia_source.phot_variable_flag,gaia_source.teff_val,gaia_source.a_g_val, 
                        gaia_source.pmra as proper_motion_ra, gaia_source.pmra_error as proper_motion_ra_error, 
                        gaia_source.pmdec as proper_motion_dec, gaia_source.pmdec_error as proper_motion_dec_error
                    FROM gaiadr2.gaia_source 
                    WHERE (gaiadr2.gaia_source.source_id=4722135642226356736 OR 
                        gaiadr2.gaia_source.source_id=4722111590409480064)"""

        output_df = self.gaia_query_to_pandas(query, **kwargs)
        if save_to_parquet:
            self.data_save_parquet(output_df, "initial_dr2_data")
        return output_df

    def gaia_get_hipp_binaries(self, save_to_parquet, only_show_stars_with_both_stars_in_data=True, **kwargs):
        """

        Function to run a query which joins Gaia DR2 and Hipparcos, and filters to stars known to be in a binary system.
            Additionally, there is a filter to only show stars where at least 2 stars in the binary system exists in
            both Hipparcos and Gaia DR2.

        Args:
            save_to_parquet (bool): Save results to Parquet format.
            only_show_stars_with_both_stars_in_data (bool): Only show stars where at least 2 stars in the binary system exists in
                both Hipparcos and Gaia DR2.

        Kwargs:
            output_file (str): optional, default None
                file name where the results are saved if dumpToFile is True.
                If this parameter is not provided, the jobid is used instead
            output_format (str): optional, default 'votable'
                results format
            verbose (bool): optional, default 'False'
                flag to display information about the process
            dump_to_file (bool): optional, default 'False'
                if True, the results are saved in a file instead of using memory
            background (bool): optional, default 'False'
                when the job is executed in asynchronous mode, this flag specifies
                whether the execution will wait until results are available
            upload_resource (str): optional, default None
                resource to be uploaded to UPLOAD_SCHEMA
            upload_table_name (str): optional, default None
                resource temporary table name associated to the uploaded resource.
                This argument is required if upload_resource is provided.
            autorun (boolean): optional, default True
                if 'True', sets 'phase' parameter to 'RUN',
                so the framework can start the job.

        Returns:
            Pandas.DataFrame: Output of the query.

        """
        filter_missing_data = " HAVING count(*) > 1 " if only_show_stars_with_both_stars_in_data else ""
        query = r"""SELECT gaia_source.source_id,gaia_source.ra,gaia_source.ra_error,gaia_source.dec,
                        gaia_source.dec_error,gaia_source.parallax,gaia_source.parallax_error,gaia_source.phot_g_mean_mag,
                        gaia_source.bp_rp,gaia_source.radial_velocity,gaia_source.radial_velocity_error,
                        gaia_source.phot_variable_flag,gaia_source.teff_val,gaia_source.a_g_val, 
                        gaia_source.pmra as proper_motion_ra, gaia_source.pmra_error as proper_motion_ra_error, 
                        gaia_source.pmdec as proper_motion_dec, gaia_source.pmdec_error as proper_motion_dec_error,
						hipp.ccdm, hipp.n_ccdm AS CCDM_History, 
						hipparcos2_best_neighbour.angular_distance AS angular_distance_between_hipp_gaia,
						num_binary.num_ccdm AS num_stars_in_binary_w_data_available

                    FROM gaiadr2.gaia_source
                        LEFT JOIN gaiadr2.hipparcos2_best_neighbour 
                            ON gaia_source.source_id = hipparcos2_best_neighbour.source_id
                            LEFT JOIN public.hipparcos_newreduction AS hipp2 
                                ON hipp2.hip = hipparcos2_best_neighbour.original_ext_source_id
                                LEFT JOIN public.hipparcos AS hipp ON hipp2.hip = hipp.hip
                                    INNER JOIN (SELECT hipp.ccdm, count(*) AS num_ccdm 
                                                    FROM public.hipparcos AS hipp 
                                                        INNER JOIN gaiadr2.hipparcos2_best_neighbour 
                                                            ON hipp.hip = hipparcos2_best_neighbour.original_ext_source_id 
                                                    GROUP BY hipp.ccdm {0}
                                               ) AS num_binary ON num_binary.ccdm = hipp.ccdm
    
                                WHERE hipp.ccdm is not null
    
                                ORDER BY hipp.ccdm asc""".format(filter_missing_data)
        output_df = self.gaia_query_to_pandas(query, **kwargs)
        if save_to_parquet:
            self.data_save_parquet(output_df, "hipp_binaries")
        return output_df


def query_random_selection(num_results):
    top_line = "TOP {0}".format(num_results) if num_results is not None else ""
    query = r"""SELECT {0} gaia_source.source_id,gaia_source.ra,gaia_source.dec,gaia_source.parallax,
            gaia_source.parallax_error,gaia_source.parallax_over_error,gaia_source.phot_g_mean_mag,
            gaia_source.phot_g_mean_flux, gaia_source.bp_rp,gaia_source.dr2_radial_velocity,
            gaia_source.dr2_radial_velocity_error, gaia_source.pseudocolour, gaia_source.dr2_rv_template_teff, 
            pmra, pmdec, l, b, ecl_lon, ecl_lat, bp_g
        FROM gaiaedr3.gaia_source 
        where gaia_source.dr2_radial_velocity is not null 
        and gaia_source.parallax is not null 
        and gaia_source.phot_g_mean_mag is not null 
        --and gaia_source.bp_rp is not null 
        and gaia_source.phot_g_mean_flux is not null 
        and gaia_source.dr2_rv_template_teff is not null 
        --and gaia_source.pseudocolour is not null 
        --and pmra is not null and pmdec is not null 0
        --and l is not null and b is not null 
        --and ecl_lon is not null and ecl_lat is not null 
        and bp_g is not null and
        gaia_source.parallax_over_error > 30 and gaia_source.dr2_radial_velocity_error < 10 order by gaia_source.random_index asc""".format(
        top_line)
    return query


def run_gaia_query(query, login_config=None):
    gaia_cls = GaiaDataAccess(login_config=login_config)
    job = gaia_cls.get_gaia_job(query)
    return job


def add_calculated_cols(gaia_df):
    gaia_df['dist_pc'] = float(1) / gaia_df['parallax']
    gaia_df['cart_x'], gaia_df['cart_y'], gaia_df['cart_z'] = utils.ra_dec_dist_to_cartesian(gaia_df['ra'],
                                                                                             gaia_df['dec'],
                                                                                             gaia_df['dist_pc'])
    gaia_df['rgb_colour'] = "rgb" + gaia_df['bp_g'].apply(utils.bv2rgb).astype(str)
    gaia_df['dot_size'] = utils.dot_size_from_mag(gaia_df['phot_g_mean_mag'])
    return gaia_df


def query_gaia_to_pandas(query, login_cnf=None):
    job = run_gaia_query(query, login_cnf)
    gaia_data = job.to_pandas()
    return gaia_data


def gaia_query_to_parquet(query, output_file_name, login_cnf=None):
    gaia_data = query_gaia_to_pandas(query, login_cnf=login_cnf)
    gaia_data.to_parquet("{0}.parquet.gzip".format(output_file_name), compression='gzip')
