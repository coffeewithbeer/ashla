import pandas as pd
import dask
import dask.dataframe as dd
from ashla import utils


class BinaryStarDataFrame(pd.DataFrame):
    """

    Proxy of a Pandas DataFrame class, with added functionality for Wide Binary Star searching.

    """

    def __init__(self, data):
        if isinstance(data, pd.DataFrame):
            if 'parallax' in data.columns and 'dist_pc' not in data.columns:
                data['dist_pc'] = 1000.0 / data['parallax']
                if 'parallax_error' in data.columns and 'dist_err_pc' not in data.columns:
                    data['dist_err_pc'] = (data['parallax_error'] / data['parallax']) * data['dist_pc']

            super().__init__(data)
            self.drop_duplicates()
        else:
            raise TypeError("You did not pass a DataFrame! Boooooo!")

    def to_df(self):
        """

        Function to convert back to Pandas DataFrame.

        Returns:
            pd.DataFrame: our instance as a DataFrame.

        """
        return pd.DataFrame(self)

    def add_cartesian_coords_cols(self):
        self['cart_x'], self['cart_y'], self['cart_z'] = utils.ra_dec_dist_to_cartesian(self['ra'],
                                                                                        self['dec'],
                                                                                        self['dist_pc'])
        return self

    def add_plotting_data_cols(self):

        self['rgb_colour'] = "rgb" + self['bp_g'].apply(utils.bv2rgb).astype(str)
        self['dot_size'] = utils.dot_size_from_mag(self['phot_g_mean_mag'])
        return self


if __name__ == '__main__':
    import ashla.data_access as da

    gaia_cnxn = da.GaiaDataAccess(r'C:\Users\zacha\Documents\ashla\ashla\data_access\login_conf_zplummer.ini')
    output_data = gaia_cnxn.gaia_get_hipp_binaries(save_to_parquet=True, only_show_stars_with_both_stars_in_data=True)
    #output_data = gaia_cnxn.load_async_job('1616446549863O')
    # data_bin = BinaryData(output_data)
    print(output_data)
