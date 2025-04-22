import numpy as np
import pandas as pd

def preprocess (dat, verbose=True):
    # parity has a lot of nan, fill with 1 and mark the nan entry
    dat['parity'] = pd.to_numeric(dat['parity'], errors='coerce')  # the type may sometimes be Object
    dat['parity_isna'] = np.where(dat['parity'].isna(), 1, 0)
    dat['parity'] = dat['parity'].fillna(1.0)

    # we will not worry about censustract data for now
    census_tract_cols = [
        'acs_median_hh_inc',
        'acs_pct_inc50',
        'acs_pct_person_inc_100_124',
        'acs_pct_person_inc_125_199',
        'acs_pct_person_inc_above200',
        'acs_pct_person_inc_below99',
        'acs_pct_college_associate_dgr',
        'acs_pct_bachelor_dgr',
        'acs_pct_no_work_no_schl_16_19',
        'acs_pct_graduate_dgr',
        'acs_pct_hs_graduate',
        'acs_pct_lt_hs',
        'acs_pct_posths_ed',
        'epl_pov150',
        'epl_unemp',
        'epl_hburd',
        'epl_nohsdp',
        'epl_uninsur',
        'rpl_theme1',
        'rpl_theme2',
        'rpl_theme3',
        'rpl_theme4',
        'rpl_themes',
        'closest_day',
        'has_address',
        'has_tractfips'
    ]
    if verbose:
        print(f"dropping {len(census_tract_cols)} of census tract columns")
    dat.drop(columns=census_tract_cols, inplace=True)

    # check for nans
    cols_to_check = dat.columns.difference(['phq9_total_max', 'edinburgh_max'])
    subset = []
    for col in cols_to_check:
        if (dat[col].isna().mean() > 0):
            if verbose:
                print(f"found columns with NaN: {col}, NaN number = {dat[col].isna().sum()}, dropping these patients")
            subset.append(col)
    dat.dropna(subset=subset, inplace=True)

    # filter out patients without prenatal care visit or postpartum visit
    condition_1 = (dat['counts_of_visits_prenatal_care'] == 0)
    condition_2 = (dat['counts_of_visits_3m_after_delivery'] == 0)
    tmp = dat[condition_1 | condition_2]
    if verbose:
        print(f"removing patients without prenatal and postpartum visit {len(tmp)}")
    dat.drop(tmp.index, inplace=True)
    
    # drop the columns after delivery
    if verbose:
        print(f"dropping columns: counts_of_visits_3m_after_delivery, counts_of_visits_6m_after_delivery")
    dat.drop(columns=['counts_of_visits_3m_after_delivery', 'counts_of_visits_6m_after_delivery'], 
             inplace=True)
    
    # drop the patients without phq or edinburgh screening
    condition_1 = dat['edinburgh_max'].isna()
    condition_2 = dat['phq9_total_max'].isna()
    tmp = dat[condition_1 & condition_2]
    if verbose:
        print(f"removing patients without screening using edinburgh or phq9 {len(tmp)}")
    dat.drop(tmp.index, inplace=True)

    # NOTE: may want to adjust these to reflect the screening tools used at your institue
    # create label using F53 or edinburgh_max or phq9
    # drop other definitions of ppd that are not used at WFU
    dat['label'] = np.where( (dat['edinburgh_max'] >= 10) | 
                             (dat['phq9_total_max'] >= 10) | 
                             (dat['F53_label'] == 1), 1, 0)
    if verbose:
        print(f"removing columns: 'F53_label','edinburgh_max','phq9_total_max', 'PPD_delete_label'")
    dat.drop(columns=['F53_label','edinburgh_max','phq9_total_max', 'PPD_delete_label'], inplace=True)
