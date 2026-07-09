# Racing All Along 🏎️
This repository is reflection of my passion for Motorsports `F1`, `WEC` and `MotoGP`. In this repository I have worked with open-source data available for each of the sports to debreif race-weekends, analyse the data to provide insight into the performance of the teams and drivers.

### Structure of the Repo 🏗️
- `F1`: Currently contains a quali, race debriefs for the Barcelona 🇪🇸 and Austria 🇦🇹 GP 2026. Legacy work include an in-depth race debrief for the [Saudi Arabian GP 🇸🇦, 2025](./F1/notebooks/legacy/saudi_2025.ipynb) and in-progress [São Paulo GP 🇧🇷, 2025](./F1/notebooks/race_debriefs_2025/brazil_2025.ipynb).
- `WEC`: Currently contains a introductory notebook for 8 Hours of Bahrain 🇧🇭, 2025. Will develop the debrief for the rest of the races from 2026 before the next race on the calendar (Brazil).

### Debriefs
The goal of a race-weekend debrief is to analyse data collected by individual teams over the weekend thereby, estimating and visualising key performance variables:
- Car Setup and Circuit Characteristics corelation.
- Car Dyanmics and the effects on Race / Quali pace.
- Post-Race strategy and tyre degradation analysis.
- Point-Estimates to outline the best driving (bloated I know however, hence the challenge) over a race weekend.

### Currently in Works
- Britain 🇬🇧, 2026 (Currently, in 🏗️)

### Upcoming 🕣
- Positively a race-debrief every week in F1 starting with Barcelona working backwards and Forwards:
    - 2026:
        - Canada 🇨🇦
        - USA - Miami 🇺🇸
        - Japan 🇯🇵
        - Monaco 🇲🇨
        - China 🇨🇳
        - Australia 🇦🇺

### Important ⚠️
- The source code used in the notebooks codenamed `Mark-1` has now migrated into its very own public repository for easier development and maintainence. It can found [here](https://github.com/anilhimam17/Mark-1)
- It can installed as follows:
    - If UV is being used:  
    ```bash
    uv sync --all-groups
    ```
    - If Pip is being used:
    ```bash
    pip install git+https://github.com/anilhimam17/Mark-1.git
    ```