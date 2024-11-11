import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV, train_test_split, StratifiedKFold
import shap
from sklearn.metrics import precision_recall_curve, roc_auc_score, f1_score, make_scorer, confusion_matrix, classification_report
from matplotlib import pyplot as plt

def fill_nans(dat):
    '''
      fill the nan in the assembled dataframe 
    '''

    # first deal with almost entire nan columns
    for col in dat.columns:
        if dat[col].isna().mean() > 0.99:
            print(f'removing column {col}, too many nan')
            dat.drop(columns=col, inplace=True)

    # deal with the datetime
    # note: all the dates are converted to string when save into csv
    cols = list(dat.select_dtypes(np.datetime64).columns)
    cols.remove('preg_start_date')
    dat['preg_start_date'] = pd.to_datetime(dat['preg_start_date'])
    for col in cols:
        dat[col] = (pd.to_datetime(dat[col]) - dat['preg_start_date']).map(lambda x: x.days if not pd.isna(x) else 300)

    # now one by one
    dat['parity_nan'] = np.where(dat['parity'].isna(), 1, 0)
    dat['parity'] = dat['parity'].fillna(1)
    dat['bmi_nan'] = np.where(dat['bmi'].isna(), 1, 0)
    dat['bmi'] = dat['bmi'].fillna(dat['bmi'].mean())

    # deal with blood pressure
    cols_to_fill_zero = ['count_with_high_systolic', 'count_with_severe_high_systolic',
                     'count_with_high_diastolic', 'count_with_severe_high_diastolic']
    cols_to_fill_avg = ['avg_systolic', 'avg_diastolic', 
                        'highest_systolic', 'second_highest_systolic', 
                        'highest_diastolic', 'second_highest_diastolic']
    for col in cols_to_fill_zero:
        if col in dat.columns:
            dat[col] = dat[col].fillna(0)
    for col in cols_to_fill_avg:
        if col in dat.columns:
            dat[col] = dat[col].fillna(dat[col].mean())


def get_fit(dat, random_state=42):
    X = dat.drop(columns=['label'])
    print(X.shape)
    y = dat['label']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=random_state, shuffle=True)
    print(y_train.value_counts(), y_test.value_counts())
    param_grid = {'min_samples_split': [10,20,30,50,100,150,200,250,300,500,1000]}
    clf = GridSearchCV(RandomForestClassifier(class_weight='balanced', random_state=random_state), 
                   param_grid, cv=StratifiedKFold(n_splits=5, shuffle=True, random_state=random_state),
                   scoring=['roc_auc', 'f1', 'average_precision'], refit='f1')
    clf.fit(X_train, y_train)
    print(clf.best_params_, clf.best_score_)
    y_pred = clf.predict(X_test)
    # print(confusion_matrix(y_test, y_pred))
    print(classification_report(y_test, y_pred))
    auc_test_score = roc_auc_score(y_test, clf.predict_proba(X_test)[:,1])
    f1_test_score = f1_score(y_test, y_pred)
    return clf, X_test, y_test, auc_test_score, f1_test_score

def split_string(s, line_width=20):
    s = ''.join(s.split('\n'))
    return '\n'.join([s[i:i+line_width] for i in range(0,len(s),line_width)])

def get_shap(clf, X, y):
    explainer = shap.TreeExplainer(clf)
    shap_values = explainer.shap_values(X, check_additivity=False)

    X.columns = [split_string(col) for col in X.columns]
    shap.summary_plot(shap_values[1], X, show=False, max_display=30)
    plt.yticks(fontsize=10)
    fig = plt.gcf()
    fig.set_figwidth(8)
    fig.set_figheight(10)

    df = pd.DataFrame({
        'feature': X.columns,
        'importance': np.mean(np.abs(shap_values[1]), axis=0)
    })
    df = df.sort_values(by='importance', ascending=False).head(30)
    df.plot.barh(y='importance', x='feature', figsize=(8,12))
    plt.gca().invert_yaxis()
    fig2 = plt.gcf()
    
    return fig, fig2
    