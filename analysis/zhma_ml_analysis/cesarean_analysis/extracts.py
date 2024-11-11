# extracts data from the database

from sqlqueries import Queries
import pandas as pd
import sqlalchemy
import time
import numpy as np
from datetime import datetime

def create_label(dat):
    ''' create label according to complication and delivery mode '''
    dat['label'] = -1

    # normal delivery
    idx = (dat.birth_counts == 1) & \
        (dat.min_sm_name.str.startswith('Vaginal, Spontan') | dat.min_sm_name.str.startswith('VBAC, Spont'))
    print(idx.sum())
    dat.loc[idx, 'label'] = 0

    # cesarean after attempted labor
    idx = (dat.birth_counts == 1) & \
        (dat.min_sm_name.str.startswith('C')) & \
        ((dat.failed_labor > 0) | (dat.other_complications > 0))
    print(idx.sum())
    dat.loc[idx, 'label'] = 1

    # drop these columns
    dat.drop(columns=['abortion','min_cpt_px_date','max_cpt_px_date',
                    'min_sm_name','max_sm_name','min_sm_px_date','max_sm_px_date','birth_counts',
                    'failed_labor','other_complications','earliest_failed_labor_date','earliest_other_complication_date'], inplace=True)

def get_prev_cesarean(dat):
    ''' get cpt indicated cesarean '''
    dat['cpt_indicated_no_prev_cesarean'] = np.where( (dat['vaginal'] > 0) | (dat['cesarean'] > 0), 1, 0)
    dat['cpt_indicated_prev_cesarean'] = np.where( (dat['VBAC'] > 0) | (dat['cesareanAC'] > 0), 1, 0)
    dat.drop(columns=['vaginal','cesarean','VBAC','cesareanAC'], inplace=True)


if (__name__ == '__main__'):
    connection_string = 'mssql+pyodbc://@ahw-corid-ms-01.medctr.ad.wfubmc.edu/ammi?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server'
    engine = sqlalchemy.create_engine(connection_string)

    queries = Queries()
    queries.update('b.delivery_date')

    print(f"starting: {datetime.now()}")

    # static part of the data
    dat_complication = pd.read_sql(queries.complication_sql_string, con=engine)
    dat_delivery = pd.read_sql(queries.delivery_sql_string, con=engine)
    dat_twin = pd.read_sql(queries.twin_sql_string, con=engine)
    dat_parity = pd.read_sql(queries.parity_sql_string, con=engine)
    dat_previous_cesarean = pd.read_sql(queries.previous_cesarean_sql_string, con=engine)
    dat_other_parity = pd.read_sql(queries.other_parity_sql_string, con=engine)
    dat_mum_age = pd.read_sql(queries.mum_age_sql_string, con=engine)
    dat_mum_height = pd.read_sql(queries.mum_height_sql_string, con=engine)
    dat_bmi = pd.read_sql(queries.bmi_sql_string, con=engine)

    # need to deal with delivery and twin separately
    all_BIRTHID = set(dat_complication.BIRTHID.values)
    dat_delivery = dat_delivery[dat_delivery.BIRTHID.map(lambda x: x in all_BIRTHID)].copy().reset_index(drop=True)
    dat_twin = dat_twin[dat_twin.BIRTHID.map(lambda x: x in all_BIRTHID)].copy().reset_index(drop=True)

    # fix the bmi
    dat_bmi['bmi'] = np.where(dat_bmi['earliest_bmi'].isna(), dat_bmi['computed_bmi'], dat_bmi['earliest_bmi'])
    dat_bmi = dat_bmi[['BIRTHID','bmi']].copy()

    assert(all(dat_delivery.BIRTHID == dat_complication.BIRTHID))
    assert(all(dat_delivery.BIRTHID == dat_twin.BIRTHID))
    assert(all(dat_delivery.BIRTHID == dat_parity.BIRTHID))
    assert(all(dat_delivery.BIRTHID == dat_previous_cesarean.BIRTHID))
    assert(all(dat_delivery.BIRTHID == dat_other_parity.BIRTHID))
    assert(all(dat_delivery.BIRTHID == dat_mum_age.BIRTHID))
    assert(all(dat_delivery.BIRTHID == dat_mum_height.BIRTHID))
    assert(all(dat_delivery.BIRTHID == dat_bmi.BIRTHID))

    dat = pd.concat([
        dat_complication,
        dat_delivery.drop(columns=['BIRTHID', 'MOTHER_ENCOUNTERID']),
        dat_twin.drop(columns=['BIRTHID']),
        dat_parity.drop(columns=['BIRTHID']),
        dat_previous_cesarean.drop(columns=['BIRTHID']),
        dat_other_parity.drop(columns=['BIRTHID']),
        dat_mum_age.drop(columns=['BIRTHID']),
        dat_mum_height.drop(columns=['BIRTHID']),
        dat_bmi.drop(columns=['BIRTHID'])
    ], axis=1)

    create_label(dat)
    get_prev_cesarean(dat)
    dat.to_parquet('dat_static.parquet', index=False)
    print(f"{datetime.now()} dat_static saved")

    intervals = {
        'time_frames': ['DATEADD(DAY, -c.gest_age_in_days + 91, b.delivery_date)',
                        'DATEADD(DAY, -c.gest_age_in_days + 196, b.delivery_date)',
                        'DATEADD(DAY, -30, b.delivery_date)',
                        'DATEADD(HOUR, -72, b.delivery_date)',
                        'DATEADD(HOUR, -24, b.delivery_date)',
                        'DATEADD(HOUR, -12, b.delivery_date)',
                        'DATEADD(HOUR, -3, b.delivery_date)',
                        'DATEADD(HOUR, -1, b.delivery_date)',
                        'b.delivery_date'],
        'file_names': ['first_trimster',
                       'second_trimster',
                       'one_month_before_delivery',
                       '72h_before_delivery',
                       '24h_before_delivery',
                       '12h_before_delivery',
                       '3h_before_delivery',
                       '1h_before_delivery',
                       'at_delivery'] 
    }
    
    for time_frame, file_name in zip(intervals['time_frames'], intervals['file_names']):

        print(f"starting {datetime.now()} filename")
        queries.update(time_frame)

        dat_counts = pd.read_sql(queries.counts_sql_string, con=engine)
        dat_diabetes = pd.read_sql(queries.diabetes_sql_string, con=engine)
        dat_preeclampsia = pd.read_sql(queries.preeclampsia_sql_string, con=engine)
        dat_bloodpressure = pd.read_sql(queries.bloodpressure_sql_string, con=engine) # this will take some time
        dat_other_obese = pd.read_sql(queries.other_obese_sql_string, con=engine)

        assert(all(dat.BIRTHID == dat_counts.BIRTHID))
        assert(all(dat.BIRTHID == dat_diabetes.BIRTHID))
        assert(all(dat.BIRTHID == dat_preeclampsia.BIRTHID))
        assert(all(dat.BIRTHID == dat_bloodpressure.BIRTHID))
        assert(all(dat.BIRTHID == dat_other_obese.BIRTHID))

        dat_track = pd.concat([
            dat,
            dat_counts.drop(columns=['BIRTHID']),
            dat_diabetes.drop(columns=['BIRTHID']),
            dat_preeclampsia.drop(columns=['BIRTHID']),
            dat_bloodpressure.drop(columns=['BIRTHID']),
            dat_other_obese.drop(columns=['BIRTHID'])
        ], axis=1)

        # finally the medications
        dat_rx = pd.read_sql(queries.get_rx_sql_string, con=engine)
        dat_rx_pivot = dat_rx.pivot(index='BIRTHID', values='total_quantity', columns='med_name')
        dat_rx_pivot = dat_rx_pivot.reset_index().fillna(0)
        dat_rx_pivot['BIRTHID'] = dat_rx_pivot['BIRTHID'].astype(str)

        dat_track = pd.merge(left=dat_track, right=dat_rx_pivot, left_on='BIRTHID', right_on='BIRTHID', how='left')
        for col in dat_rx_pivot.drop(columns=['BIRTHID']).columns:
            dat_track[col] = dat_track[col].fillna(0)
        
        # now save this file
        dat_track.to_parquet(f'dat_track_{file_name}.parquet', index=False)
        print(f"{datetime.now()} track file {file_name} saved")