import os
import lemlab


def update_x_files():
    import json

    list_scenarios = os.listdir("./scenarios")
    list_already_run = os.listdir("./simulation_results")

    with open(f"X:/Nutzer/slumpp/ll_scenarios.json", "r") as read_file:
        x_list_scen = json.load(read_file)
    for scen in list_scenarios:
        if scen not in x_list_scen:
            x_list_scen.append(scen)
    with open(f"X:/Nutzer/slumpp/ll_scenarios.json", "w") as write_file:
        json.dump(x_list_scen, write_file)

    with open(f"X:/Nutzer/slumpp/ll_results.json", "r") as read_file:
        x_list_results = json.load(read_file)
    for scen in list_already_run:
        if scen not in x_list_results:
            x_list_results.append(scen)

    sim_name = None
    for scen in x_list_scen:
        if scen not in x_list_results:
            sim_name = scen
            x_list_results.append(scen)
            break

    with open(f"X:/Nutzer/slumpp/ll_results.json", "w") as write_file:
        json.dump(x_list_results, write_file)

    print(f"{len(x_list_scen) - len(x_list_results)}/{len(x_list_scen)} scenarios remaining...")
    print(f"Now starting simulation of {sim_name}.")
    return sim_name


if __name__ == "__main__":
    simulation = lemlab.ScenarioExecutor(path_scenario=f"./scenarios/case_study_4_2_pro_1_hor_44_t_shift_48",
                                         path_results=f"./simulation_results/case_study_4_2_pro_1_hor_44_t_shift_48")
    simulation.run()
    # random.seed(14)
    #
    # flag_continue = 1
    # while flag_continue:
    #     sim_name = update_x_files()
    #     if sim_name is not None:
    #         simulation = lemlab.ScenarioExecutor(path_scenario=f"./scenarios/{sim_name}",
    #                                              path_results=f"./simulation_results/{sim_name}")
    #         simulation.run()
    #     else:
    #         flag_continue = 0
