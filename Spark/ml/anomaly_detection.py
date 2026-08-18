import pandas as pd
import numpy as np


def detect_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    metrics = ['signal_strength', 'latency_ms', 'packet_loss', 'server_load_cpu']
    result = df.copy()
    for metric in metrics:
        mean = result[metric].mean()
        std = result[metric].std(ddof=0)
        if std == 0:
            result[f'{metric}_anomaly'] = False
        else:
            result[f'{metric}_anomaly'] = np.abs(result[metric] - mean) > 3 * std
    result['is_anomaly'] = result[[f'{metric}_anomaly' for metric in metrics]].any(axis=1)
    return result


if __name__ == '__main__':
    sample = pd.read_csv('data/ran_telemetry.csv')
    anomalies = detect_anomalies(sample)
    print(anomalies[['timestamp', 'cell_id', 'region', 'is_anomaly']].head(20))
