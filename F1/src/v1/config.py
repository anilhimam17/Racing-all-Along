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

# Considering Hybrid as a 48% split
HYBRID_POWER = 0.47

# Fuel Sample Limit
FUEL_SAMPLE_LIMIT = 3

# ======================= Circuit Specific Configurations =======================

# The Percentage of Time on Full Throttle by Circuit
THROTTLE_CYCLE = {
    "barcelona": 0.7
}

# The Distance the Longest Straight for a Given Circuit
STRAIGHTS = {
    "barcelona": 0.44   # in KM by Al Kamel systems
}