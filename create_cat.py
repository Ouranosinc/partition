""" create catalog with indicators from repo """
import os
from pathlib import Path
if 'ESMFMKFILE' not in os.environ:
    os.environ['ESMFMKFILE'] = str(Path(os.__file__).parent.parent / 'esmf.mk')
import xscen as xs
from xscen import CONFIG
import numpy as np
path = 'config/path_part.yml'
config = 'config/config_part.yml'
xs.load_config(path, config, verbose=(__name__ == '__main__'), reset=True)

if __name__ == '__main__':

    # create project catalog
    pcat = xs.ProjectCatalog(
        CONFIG['project_catalog']['path'],
        create=True,
        project=CONFIG['project_catalog']['project']
    )

    df = xs.catutils.parse_directory(directories=[CONFIG['paths']['published_data']],
                                     patterns=['{bias_adjust_project}/{xrfreq}/{bias_adjust_project}_{mip_era}_{activity}_{institution}_{source}_{experiment}_{member}_{?}/{bias_adjust_project}_{mip_era}_{activity}_{institution}_{source}_{experiment}_{member}_{?}_{domain}_{processing_level}_{xrfreq}_{variable:_}.zarr',],
                                     homogenous_info={'date_start': '1950-01-01',
                                                      'date_end': '2100-12-31'}
                                     )
    pcat.update(df)


    def add_col(row, d):
        """
        Add a column based on the bias_adjust_project column and a dict of translations.
        """
        if not isinstance(row['bias_adjust_project'], str):
            return np.nan

        for k, v in d.items():
            if k in row['bias_adjust_project']:
                return v

    # add reference and adjustment to the catalog
    df = pcat.df.copy()
    pcat.df['adjustment'] = df.apply(add_col, axis=1, d=CONFIG['translate']['adjustment'])
    pcat.df['reference'] = df.apply(add_col, axis=1, d=CONFIG['translate']['reference'])
    pcat.update()