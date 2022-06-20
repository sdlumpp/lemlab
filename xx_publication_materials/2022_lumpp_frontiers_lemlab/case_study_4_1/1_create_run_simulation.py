import random
import lemlab

if __name__ == "__main__":
    random.seed(14)
    sim_name = f"case_study_4_1"
    scenario = lemlab.Scenario()
    scenario.new_scenario(path_specification="0_config_case_study_4_1.yaml",
                          scenario_name=f"{sim_name}")

    simulation = lemlab.ScenarioExecutor(path_scenario=f"./scenarios/{sim_name}",
                                         path_results=f"./simulation_results/{sim_name}")
    simulation.run()
