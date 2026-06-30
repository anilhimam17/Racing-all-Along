# ======================= Assumed Constant for Pace Grounding =======================
# Fuel Load
MAX_FUEL_LOAD_IN_KG = 70

# Fuel Flow
MAX_FUEL_FLOW_IN_KGH = 100
FUEL_FLOW_CONV_CONST = 1000 / 3600

# Fuel Save for Race Trim pace
FUEL_STRAT = 5

# Car Weight without Fuel with Driver (assumed optimal)
CAR_WEIGHT_IN_KG = 772

# Weight to Time Conversion => 0.3s every 10kg
WEIGHT_TIME_CONV_CONST = 0.3 / 10

# KM-HR to M-S Time Conversion
MS_CONV_CONST = 5 / 18

# Considering Hybrid as a 48% split
HYBRID_POWER = 0.48

# Fuel Sample Limit
FUEL_SAMPLE_LIMIT = 3

# ======================= Circuit Specific Configurations =======================

# The Distance the Longest Straight for a Given Circuit
STRAIGHTS = {
    "barcelona": 0.44   # in KM by Al Kamel systems
}

# ======================= Data Visualisation Configurations =======================

SECTOR_MAPS = {
    "Sector1": ("SpeedI1", "Sector1Time", "KineticEnergyS1_KJ"),
    "Sector2": ("SpeedI2", "Sector2Time", "KineticEnergyS2_KJ"),
    "Sector3": ("SpeedFL", "Sector3Time", "KineticEnergyS3_KJ")
}

AERO_VIS_CONFIG = [
    ("Sector1Time", "FrontAEI", "Sector 1\nHigh Speed, Downforce / Drag Tradeoff"), 
    ("Sector2Time", "BalancedAEI", "Sector 2\nMedium Speed, High Downforce & Minimal Drag"), 
    ("Sector3Time", "RearAEI", "Sector 3\nMedium - Low Speed, High Downforce")
]

ERS_VIS_CONFIG = [
    ("AccelerationTime", "ERS_Clipping", "")
]

KE_VIS_CONFIG = [
    ("Driver", "KineticEnergyS1_KJ", "Sector 1\nHigh Speed, Downforce / Drag Tradeoff"),
    ("Driver", "KineticEnergyS2_KJ", "Sector 2\nMedium Speed, High Downforce & Minimal Drag"),
    ("Driver", "KineticEnergyS3_KJ", "Sector 3\nMedium - Low Speed, High Downforce"),
]

POWER_VIS_CONFIG = [
    ("Driver", "PowerS1_KW", "Sector 1\nHigh Speed, Downforce / Drag Tradeoff"),
    ("Driver", "PowerS2_KW", "Sector 2\nMedium Speed, High Downforce & Minimal Drag"),
    ("Driver", "PowerS3_KW", "Sector 3\nMedium - Low Speed, High Downforce")
]