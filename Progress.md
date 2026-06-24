## Currently being Implemented

1. Aerodynamic & Drag Balance Setup Analysis - `Mostly Implemented in Qualifying Analysis`
    * Quali vs. Race Setup Proxy:
        * Compare the speed trap SpeedST and sector times between Q3 (single flying lap) and the race (high-fuel/hybrid deployment).
            * High-Drag Setup: A car that is exceptionally slow in SpeedST but dominant in Sector 3 (slow speed chassis cornering) has run a high-downforce "barn door" wing setup. This
                manages tyres beautifully in the race but leaves them vulnerable to being overtaken on the straight.
            * Low-Drag Setup: A car hitting massive speeds in SpeedST but bleeding time in Sector 3 is running a skinny wing. They will have poor tyre life but high overtaking defense.
        * Aero Efficiency Index (AEI): Compare top-speed capability against slow cornering performance:
        AEI = (CleanAir Mean (SpeedST))/(CleanAir Mean (Sector 3 Time))

---

2. ICE Power and Hybrid Efficiency Index (IPHEI) - `Currently being Implemented`
    - With the 2026 regulations enforcing a near 50-50 power split between mechanical ICE (capped at ~400 kW) and electrical ERS (surging to 350 kW), PU performance is dominated by electrical harvesting and deployment efficiency. 
    - If a PU cannot harvest energy efficiently, it will experience "clipping" (running out of battery and dropping power before the end of the straight).
    - To isolate them, we exploit the 2026 ERS-K De-rating (Derating) Curve:
        * The rule: The MGU-K is permitted to deploy the full 350 kW up to 340 km/h. Above 340 km/h, the regulations mandate that the electrical power de-rates linearly (down to 150 kW
        at 355 km/h).
        * The physical constraint: Below 290 km/h, acceleration is heavily dominated by the unrestricted 350 kW of the MGU-K. Above 320 km/h, the electrical deployment is severely restricted, and further acceleration is almost entirely driven by the ICE's mechanical horsepower vs. the car's Aerodynamic Drag (C_d A).
    - ICE Power Index:
        - ICE PI_{isolated} = \frac{(1)/(2) m (SpeedST² - SpeedFL²)}{D_{Acc}}
        - This evaluates the acceleration in the de-rated zone (from $SpeedFL$ to $SpeedST$): Because this acceleration occurs entirely at high velocities (> 295 km/h up to 336 km/h), the MGU-K is heavily restricted by the de-rating regulations. 
        - Consequently, this index is a pure reflection of ICE power and aerodynamic drag efficiency.
    - MGUK Torque Index:
        - MSTI = \frac{(1)/(2) m (SpeedFL² - SpeedI2²)}{Δ t_{S3}} 
        - Evaluate the acceleration from corner apex to the start of the straight (using Sector 3 times and $SpeedFL$) This captures the low-to-mid speed traction phase where the hybrid system is deploying maximum torque.
    - ERS Clipping Ratio: 
        - (ECR) = (SpeedST)/(SpeedFL)
        - If ECR ≈ 1.0, the car is experiencing high clipping (battery running out, limiting top speed). 
        - If ECR is high (e.g., $> 1.15$), the hybrid system is deploying sustained energy all the way down the straight.
    - Hybrid Efficiency Index:
        - (HEI) = \frac{Mean SpeedST_{Race, CleanAir}}{SpeedST_{Qualifying}} (Scope for better modelling)

---

3. Pit Stop & Strategy Window Efficiency - `Yet to Implement`
    * The In-Lap / Out-Lap Delta:
        * Track the Out-Lap performance (PitOutTime lap) of a driver on fresh tyres against the old-tyre pace of their rival who stayed out. This measures the Undercut/Overcut Delta
            in real-time.
    * Pit Lane Loss (The "Delta to Clean Air"):
        * PitLaneLoss = In-LapTime + Out-LapTime - 2 × AverageCleanAirLapTime.
        * Subtracting the standard stationary pitstop time (from timing apps) from this loss gives the In-Lap and Out-Lap driver execution efficiency.

---

4. CTO Addition: The "Release Pace" (Gap Analysis) - `Yet to Implement`
    * By tracking Position and the gap to the car ahead over time, we can calculate "Release Pace":
        * When a driver is stuck in a DRS train (gaps < 1.0s), we compute their pace.
        * The moment they pit or the car ahead pits, we measure their pace on the subsequent 3 laps in clean air. The delta represents the Traffic Penalty Coefficient for that chassis. 

---

5. Dirty Air vs. Slipstream Flagging System (DASS) - Yet to Implement
    - Dirty Air: A trailing car (TrafficDelta < 1.5s) experiences turbulent wake destroying downforce in corners
    - Slipstream: A trailing car experiences reduce drag boosting straight-line speed. 

    ```python
    def classify_aerodynamic_state(row, clean_air_baselines):
    """
    Classifies the aerodynamic state of a lap.
    clean_air_baselines: dict containing mean SpeedST, Sector1Time, and Sector3Time
                        in clean air for each driver/compound.
    """
    driver = row["Driver"]
    compound = row["Compound"]
    delta = row["TrafficDelta"]

    if delta == 0.0 or delta >= 1.5:
        return "CLEAN_AIR"

    # Extract baseline clean air performance
    baseline_speed = clean_air_baselines[driver][compound]["SpeedST"]
    baseline_s1 = clean_air_baselines[driver][compound]["Sector1Time"]
    baseline_s3 = clean_air_baselines[driver][compound]["Sector3Time"]

    # Check straight line slipstream (SpeedST higher than clean air baseline)
    slipstream_active = row["SpeedST"] > (baseline_speed + 3.0) # Speed delta > 3km/h

    # Check dirty air cornering penalty (Corner times slower than baseline)
    dirty_air_active = (row["Sector1Time"] > (baseline_s1 + 0.2)) or (row["Sector3Time"] > (baseline_s3 + 0.2))

    if slipstream_active and dirty_air_active:
        return "DIRTY_AIR_WITH_SLIPSTREAM"  # Classic close following (S1/S3 compromised, ST elevated)
    elif slipstream_active:
        return "PURE_SLIPSTREAM"            # Straight-line draft with no cornering loss
    elif dirty_air_active:
        return "PURE_DIRTY_AIR"             # Cornering loss without straight-line draft
    else:
        return "CLOSE_FOLLOWING_NEUTRAL"    # Trailing but no significant physics delta
    ```

---

## Todo Remaining

### Initial Todo Iteration
1) Start of Stint Pace and End of Stint Pace
    - First 5 laps average vs final 5 laps average per stint
    - Sector times on fresh tyres establish baseline
    - Fuel/energy deployment rate in first stint (if available)
    - Degradation rate (sec per lap)
    - Sector times breakdown shows where you're losing time
    - Compare degradation curves between compounds. Plot each driver's last lap vs fresh pace to show delta. Use sector times to identify where the degradation is concentrated.

---

2) Track Time Evolution (Sector-by-Sector Pace)
    - Critical: Plot every lap's sector times over race
    - Shows where track is warming, drying, or cooling
    - If S1 improving but S2 falling - degradation issue
    - If all sectors rising - track heating up

---

### Offloaded Features (Dependent on the availability of Telemetry)
1) Energy per Lap
    - Energy or ES (energy status) if present in laps/telemetry
    - Compare lap time increases against energy deployment rate
    - Sector times help correlate where energy saving vs usage happened
    - Plot degradation curves for same compound drivers against each other - this reveals relative efficiency. The "why" of sector time deltas explains the energy cost.

---

2) Driver Position vs Pace Correlation
    - Who's pace improving vs who's degrading?
    - Front running strategy vs back marking degradation management
    - Can you quantify the gap closure rate per lap?
