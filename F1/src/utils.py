# Driver Number to Short Name mapping for downstream tasks.
DRIVER_NUMBER_MAP = {
    "1": "VER",
    "81": "PIA",
    "63": "RUS",
    "16": "LEC",
    "12": "ANT",
    "55": "SAI",
    "44": "HAM",
    "22": "TSU",
    "10": "GAS",
    "4": "NOR",
    "23": "ALB",
    "30": "LAW",
    "14": "ALO",
    "6": "HAD",
    "87": "BEA",
    "18": "STR",
    "7": "DOO",
    "27": "HUL",
    "31": "OCO",
    "5": "BOR"
}

# Identified Keypoints in Telemetry for Windowing the Samples
TELEMETRY_KEYPOINTS_BY_DIST = {
    "Lap_start_top_speed": 0,
    "T1_braking_stability": 100,
    "T1_in": 250,
    "T2_out": 500,
    "T3_acc": 875,
    "T4_in": 1270,
    "T5_out": 1380,
    "T5_acc": 1650,
    "T6_in": 1920,
    "T7_out": 2110,
    "T8_in": 2190,
    "T8_out": 2300,
    "T9_in": 2360,
    "T9_out": 2420,
    "T10_acc": 2540,
    "T10_in": 2600,
    "T10_out": 2700,
    "T11_load": 2875,
    "T12_in": 3080,
    "T12_out": 3190,
    "T13_load": 3260,
    "Home_acc": 3755,
    "Home_top_speed": 4100
}

# Segregating the Keypoints based on Average Load and Maximum Load on the Car
# Avg Load: Corner Entry and Exit (bar braking load)
AVG_LOAD_KEYS = [
    "T1_in", "T2_out", "T4_in", "T5_out", "T6_in", 
    "T7_out", "T8_in", "T8_out", "T9_in", "T9_out",
    "T10_in", "T10_out", "T12_in", "T12_out"
]

# Max Load: Traction Zones and Acceleration Zones
MAX_LOAD_KEYS = [
    'Lap_start_top_speed', 'T1_braking_stability', 'T3_acc', 'T5_acc', 
    'T10_acc', 'T11_load', 'T13_load', 'Home_acc', 'Home_top_speed'
]

# Braking Load: Braking Zones
BRAKING_KEYS = [
    "T1_braking_stability", "T1_in", "T4_in", "T6_in",
    "T8_in", "T9_in", "T10_in", "T12_in"
]

# Drop Columns for the Baseline Multivariate Dataframe
MULTIVARIATE_DROP_COLS = [
    "Time", "PitInTime", "PitOutTime", "Sector1SessionTime", "Sector2SessionTime",
    "Sector3SessionTime", "SpeedI1", "SpeedI2", "SpeedFL", "SpeedST",
    "IsPersonalBest", "Compound", "FreshTyre", "Team", "LapStartTime", "LapStartDate", 
    "TrackStatus", "Deleted", "DeletedReason", "FastF1Generated", "IsAccurate"
]