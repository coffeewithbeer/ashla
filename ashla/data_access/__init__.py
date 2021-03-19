from astroquery.gaia import Gaia
from ashla import utils
import ashla.data_access.config as cnf


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
    if login_config is None:
        # You can type in your details
        Gaia.login()
    else:
        login_conf_inst = cnf.GaiaLoginConf(config_file=login_config)
        Gaia.login(user=login_conf_inst.user, password=login_conf_inst.password)
    j1 = Gaia.launch_job(query)
    job = j1.get_results()
    return job


def add_calculated_cols(gaia_df):
    gaia_df['dist_pc'] = float(1) / gaia_df['parallax']
    gaia_df['cart_x'], gaia_df['cart_y'], gaia_df['cart_z'] = utils.ra_dec_dist_to_cartesian(gaia_df['ra'],
                                                                                             gaia_df['dec'],
                                                                                             gaia_df['dist_pc'])
    gaia_df['rgb_colour'] = "rgb" + gaia_df['bp_g'].apply(utils.bv2rgb).astype(str)
    gaia_df['dot_size'] = utils.dot_size_from_mag(gaia_df['phot_g_mean_mag'])
    return gaia_df


def get_data(num_results=50000, login_cnf=None):
    job = run_gaia_query(query_random_selection(num_results), login_cnf)
    gaia_data = job.to_pandas()
    data_to_parquery(num_results, gaia_data)
    gaia_data = add_calculated_cols(gaia_data)
    return gaia_data


def query_gaia_to_pandas(query, login_cnf=None):
    job = run_gaia_query(query, login_cnf)
    gaia_data = job.to_pandas()
    return gaia_data


def gaia_query_to_parquet(query, output_file_name, login_cnf=None):
    gaia_data = query_gaia_to_pandas(query, login_cnf=login_cnf)
    gaia_data.to_parquet("{0}.parquet.gzip".format(output_file_name), compression='gzip')


def data_to_parquery(num_stars, data=None):
    output_path = "gaia_data_{0}_results.parquet.gzip".format(num_stars)
    data.to_parquet(output_path, compression='gzip')
    return data, output_path
