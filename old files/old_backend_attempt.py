#ALL THE BACKEND FUNCTIONS (NEED TO FIX LIBRARY CALLS AND ASSOCIATED SYNTAX BUT THE WORKFLOW IS GOOD)
import pandas as pd
import json
from palantir.datasets.ontology import ObjectSet, get_object_type
from palantir.ai import AIClient
from palantir.actions import ActionClient, ActionRequest, ActionStatusResponse

# FUNCTION 1: FINDING AND STORING EQUIPMENT DATSETS
def load_equipment_datasets():
    rackets = pd.read_csv(
        "/Joseph Pongonthara-9f8d80/Personal Pro Shop/tennis_rackets.csv"
    )
    strings = pd.read_csv(
        "/Joseph Pongonthara-9f8d80/Personal Pro Shop/tennis_strings.csv"
    )
    return rackets, strings

# FUNCTION 2: LOADING USER PROFILE PARAMETERS BASED ON USER_ID
def load_user_profile(user_id):
    user_profile_type = get_object_type(
        "User Profile"
    )  # Confirm the exact type name in Foundry

    user_profile_objects = (
        ObjectSet()
        .filter(type=user_profile_type, property_filters={"user_id": user_id})
        .get_objects()
    )
    if not user_profile_objects:
        raise ValueError(f"No user profile found for user_id: {user_id}")

    profile = user_profile_objects[0]
    return {
        "user_id": profile.properties.get("user_id"),
        "aspects_to_optimize": profile.properties.get("aspects_to_optimize"),
        "equipment_type": profile.properties.get("equipment_type"),
        "play_frequency": profile.properties.get("play_frequency"),
        "play_style": profile.properties.get("play_style"),
        "preferred_brand": profile.properties.get("preferred_brand"),
    }

# FUNCTIONS 3 & 4: FILTERING RACKET & STRING DATASETS
def filter_racket_data(racket_df, user):
    if user["equipment_type"] in ["racket", "both"]:
        if user["preferred_brand"]:
            brands = [b.strip().lower() for b in user["preferred_brand"].split(",")]
            racket_df = racket_df[racket_df["Brand"].str.lower().isin(brands)]

        for aspect in user["aspects_to_optimize"]:
            if aspect == "Power":
                racket_df = racket_df[
                    racket_df["Head_size"].str.extract(r"(\d{2,3})").astype(float)[0]
                    >= 100
                ]
                racket_df = racket_df[
                    ~racket_df["Stiffness"].str.lower().isin(["high", "very high"])
                ]
                racket_df = racket_df[
                    ~racket_df["Recommended_tension"].str.startswith("55")
                ]
                racket_df = racket_df[
                    racket_df["Power_level"]
                    .str.lower()
                    .isin(["medium", "medium-high", "high"])
                ]

            elif aspect == "Control":
                racket_df = racket_df[
                    racket_df["Head_size"].str.extract(r"(\d{2,3})").astype(float)[0]
                    <= 100
                ]
                racket_df = racket_df[
                    racket_df["Stiffness"]
                    .str.lower()
                    .isin(["average", "high", "very high"])
                ]
                racket_df = racket_df[
                    racket_df["Recommended_tension"].str.contains("60")
                ]
                racket_df = racket_df[
                    racket_df["Power_level"]
                    .str.lower()
                    .isin(["low", "low-medium", "medium"])
                ]

            elif aspect == "Spin":
                racket_df = racket_df[
                    racket_df["Head_size"].str.extract(r"(\d{2,3})").astype(float)[0]
                    >= 100
                ]
                racket_df = racket_df[
                    racket_df["Stiffness"].str.lower().isin(["average", "high"])
                ]
                racket_df = racket_df[
                    ~racket_df["Recommended_tension"].str.contains("40")
                ]
                racket_df = racket_df[
                    racket_df["Power_level"]
                    .str.lower()
                    .isin(["low-medium", "medium", "medium-high"])
                ]

            elif aspect == "Durability":
                good_compositions = [
                    "Graphite / Tungsten",
                    "Braided Graphite",
                    "Braided Graphite & Basalt",
                    "Basalt",
                    "Graphite Basalt Matrix",
                    "Graphite/Kevlar/BLX",
                    "Carbon Fiber Graphite",
                    "Countervail/Graphite",
                    "Sonic Core/Graphite",
                    "Sonic Core VG/Graphite",
                ]
                racket_df = racket_df[racket_df["Composition"].isin(good_compositions)]
                if user["play_frequency"] >= 3:
                    extra_comps = [
                        "Kevlar",
                        "Tungsten",
                        "Countervail",
                        "Carbon Fiber Graphite",
                    ]
                    racket_df = racket_df[
                        racket_df["Composition"].str.contains(
                            "|".join(extra_comps), case=False
                        )
                    ]

    return racket_df


def filter_string_data(string_df, user):
    if user["equipment_type"] in ["string", "both"]:
        for aspect in user["aspects_to_optimize"]:
            if aspect == "Power":
                string_df = string_df[string_df["ref_ten_lbs"] == 40]
                string_df = string_df[string_df["Stiffness_lbin"] <= 250]
                string_df = string_df[
                    string_df["Material"]
                    .str.lower()
                    .isin(["gut", "nylon", "nylon/polyurethane"])
                ]
                string_df = string_df[string_df["Tension_Loss"] <= 20]
                string_df = string_df[string_df["swing_speed"].isin(["slow", "medium"])]

            elif aspect == "Control":
                string_df = string_df[string_df["reference_tension"] == 62]
                string_df = string_df[string_df["stiffness"] >= 200]
                string_df = string_df[
                    string_df["material"].isin(["polyester", "nylon/polyester"])
                ]
                string_df = string_df[string_df["tension_loss"] <= 15]
                string_df = string_df[string_df["swing_speed"].isin(["medium", "fast"])]

            elif aspect == "Spin":
                string_df = string_df[string_df["reference_tension"] == 51]
                string_df = string_df[
                    string_df["material"].isin(["polyester", "nylon/polyester"])
                ]
                string_df = string_df[string_df["stiffness"] >= 200]
                string_df = string_df[string_df["swing_speed"].isin(["medium", "fast"])]
                string_df = string_df[string_df["spin_potential_ratio"] >= 5.0]

            elif aspect == "Durability":
                materials = [
                    "polyester",
                    "nylon/polyester",
                    "nylon/zyex",
                    "polyolefin",
                    "nylon/polyolefin",
                ]
                string_df = string_df[string_df["material"].isin(materials)]
                string_df = string_df[string_df["tension_loss"] <= 15]
                string_df = string_df[string_df["stiffness"] >= 225]
                string_df = string_df[string_df["swing_speed"].isin(["medium", "fast"])]
                if user["play_frequency"] >= 3:
                    high_dur = ["polyester", "nylon/polyolefin", "nylon/zyex"]
                    string_df = string_df[string_df["material"].isin(high_dur)]

    return string_df


# FUNCTION 5 IS GENERATING THE PROMPT USING THE USER PROFILE AND INCORPORATING
# THE FILTERED DATASETS INTO THE CODE
def generate_prompt(profile, filtered_rackets, filtered_strings):
    prompt = f"""
            {profile["user_id"]} is looking to get a new tennis {profile["equipment_type"]}.\n
            They prefer brand(s): {profile["preferred_brand"]}.\n
            They are optimizing for: {", ".join(profile["aspects_to_optimize"])}.\n
            They play {profile["play_frequency"]} times a week with a {profile["skill_level"]} style.\n
            Based on the datasets, recommend 3–5 options for their equipment.\n\n

            Choose from these rackets:
            {filtered_rackets.to_markdown(index=False)}
            Choose from these strings:
            {filtered_strings.to_markdown(index=False)}

            Format your answer in **exactly** this JSON structure:
            {{
                "racket_id": "<racket_id>",
                "string_id": "<string_id>",
                "llm_explanation": "<short explanation>"
            }}
            """
    return prompt


# FUNCTION 6 MAKES THE LLM CALL AND STORES THE OUTPUT FOR ACTION APPLY
def get_llm_recommendation(prompt):
    ai_client = AIClient()

    response = ai_client.chat_completion(
        model="gpt-4",
        messages=[
            {
                "role": "system",
                "content": "You are a helpful assistant that recommends tennis gear.",
            },
            {"role": "user", "content": prompt},
        ],
        temperature=0.4,  # balanced number
    )
    llm_text = response.choices[0].message.content.strip()

    try:
        parsed = json.loads(llm_text)
    except json.JSONDecodeError as e:
        raise ValueError(f"LLM response is not valid JSON:\n{llm_text}") from e

    racket_id = parsed.get("racket_id")
    string_id = parsed.get("string_id")
    llm_explanation = parsed.get("llm_explanation")

    return racket_id, string_id, llm_explanation


# FUNCTION 7 & 8 EXECUTION THE ACTION TYPE AND NECESSARY LOGIC
def execute_ontology_action(
    action_type_reference, parameters, wait_for_completion=True
):
    client = ActionClient()

    request = ActionRequest(
        action_type_reference=action_type_reference, parameters=parameters
    )

    action_response = client.execute(request)

    if wait_for_completion:
        action_response = client.wait_for_completion(action_response.action_run_id)

    return action_response


def create_recommendation_result(prompt):
    racket_id, string_id, llm_explanation = get_llm_recommendation(prompt)

    parameters = {
        "racket_id": racket_id,
        "string_id": string_id,
        "llm_explanation": llm_explanation,
    }

    action_type_reference = "create-recommendation-result"  # may need to change syntax

    response = execute_ontology_action(
        action_type_reference=action_type_reference,
        parameters=parameters,
        wait_for_completion=True,
    )
    print(json.dumps(response.to_dict(), indent=2))

    return response


# MAIN LOGIC
def main(user_id):
    profile = load_user_profile(user_id)
    rackets_df, strings_df = load_equipment_datasets()
    filtered_rackets = filter_racket_data(rackets_df.copy(), profile)
    filtered_strings = filter_string_data(strings_df.copy(), profile)
    prompt = generate_prompt(profile, filtered_rackets, filtered_strings)
    response = create_recommendation_result(prompt)

    return response


if __name__ == "__main__":
    user_id_input = "12345"  # Replace with actual user_id
    main(user_id_input)

