import random
import lemlab
import json
import shutil
import feather as ft

if __name__ == "__main__":
    random.seed(14)
    path_base_config = "0_config_case_study_4_2.yaml"
    base_sim = f"case_study_4_2"
    scenario = lemlab.Scenario()
    scenario.new_scenario(path_specification="0_config_case_study_4_2.yaml",
                          scenario_name=f"{base_sim}")

    for prosumer in [1, 4, 8, 27]:
        for trading_horizon in range(4, 96, 8):
            for temporal_shift in range(-18*4, 24*4, 3*4):
                id_prosumer = str(prosumer).zfill(10)
                sim_name = f"{base_sim}_pro_{prosumer}_hor_{trading_horizon}_t_shift_{temporal_shift}"
                path_scenario = f"./scenarios/{sim_name}/"
                path_prosumer = f"{path_scenario}/prosumer/{id_prosumer}/"
                print(f"*** {sim_name} successfully created ***")

                shutil.rmtree(path_scenario,
                              ignore_errors=True)

                shutil.copytree(src=f"./scenarios/{base_sim}",
                                dst=path_scenario)

                with open(f"{path_prosumer}/config_account.json", "r") as config_file:
                    config = json.load(config_file)

                config["ma_horizon"] = trading_horizon

                with open(f"{path_prosumer}/config_account.json", 'w') as write_file:
                    json.dump(config, write_file)

                with open(f"{path_prosumer}/config_plants.json", "r") as plant_config:
                    plant_config = json.load(plant_config)

                id_hh_load = 0
                for key in plant_config:
                    if plant_config[key]["type"] == "hh":
                        id_hh_load = key

                hh_raw_data = ft.read_dataframe(f"{path_prosumer}/raw_data_{id_hh_load}.ft")
                hh_raw_data["power"] = hh_raw_data["power"].shift(temporal_shift)
                hh_raw_data.fillna(0)
                ft.write_dataframe(hh_raw_data, f"{path_prosumer}/raw_data_{id_hh_load}.ft")

    shutil.rmtree(f"./scenarios/{base_sim}",
                  ignore_errors=True)
