from typing import List
import pandas as pd

def adaptive_y(max_y, step_num=5):
    # max_y_str_length = len(str(max_y))
    # first_digital = int(str(max_y)[0])
    # if first_digital < 3:
    #     step = 4 * 10 ** (max_y_str_length - 2)
    # elif first_digital < 5:
    #     step = int(0.5*10 ** (max_y_str_length - 1))
    # elif first_digital < 7:
    #     step = int(1 * 10 ** (max_y_str_length - 1))
    # elif first_digital < 9:
    #     step = int(1.5 * 10 ** (max_y_str_length - 1))
    # else:
    #     step = 1 * 10 ** (max_y_str_length - 1)
    step = (max_y // step_num) 
    step_length = len(str(step))
    print(step)
    step = step // (10 ** (step_length - 2))
    print(step)
    step = step * (10 ** (step_length - 2))
    print(step)
    return step


def to_fomat(s: str, pre: bool=False) -> str:
    if s.lower() == 'sparkle original': return 'Sparkle'
    if s.lower() == 'sparkle partial' and not pre: return 'Spectrum'
    if s.lower() == 'sparkle partial' and pre: return 'Spectrum$_\mathit{p}$'
    if s.lower() == 'sparkle partial-v2': return 'Spectrum$_\mathit{pp}$'
    if s.lower() == 'aria fb': return 'AriaFB'
    if s.lower() == 'serial': return 'Serial'
    if s.lower() == 'ycsb': return 'Y'
    if s.lower() == 'smallbank': return 'S'
    return s

def order_handles_labels(handles, labels: List):
    label_order = labels
    if 'Serial' in labels:  label_order = ['Spectrum', 'AriaFB', 'Sparkle', 'Serial']
    if 'Spectrum$_\mathit{pp}$' in labels:  label_order = ['Spectrum$_\mathit{pp}$', 'Sparkle', 'Spectrum$_\mathit{p}$', 'AriaFB']
    return [handles[i] for i in [labels.index(label) for label in label_order]], label_order

def add_serial(recs: pd.DataFrame, x: str, value: int):
    for _ in sorted(list(set(recs[x]))):
        if value:
            recs.loc[len(recs.index)] = { 'protocol': 'serial', x: _, 'average commit': value }
        else:
            recs.loc[len(recs.index)] = { 'protocol': 'serial', x: _, 'average commit': 28606.4 } #25301.0 } # 24398
        # recs.loc[len(recs.index)] = { 'protocol': 'serial', x: _, 'average commit': 73408.0 }

    recs.drop(index=recs[recs['threads'] == 1].index, inplace=True)
