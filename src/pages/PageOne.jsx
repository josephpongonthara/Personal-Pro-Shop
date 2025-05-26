import React, { useState } from "react";
import { useNavigate } from "react-router-dom";
import "../App.css";

const PageOne = () => {
  const navigate = useNavigate();
  const [formState, setFormState] = useState({
    name: "",
    equipment: "",
    brands: [],
    optimize: [],
    playStyle: "",
    frequency: ""
  });
  const [step, setStep] = useState(1);
  const [loading, setLoading] = useState(false);

  const handleChange = (field, value) => {
    setFormState(prev => ({ ...prev, [field]: value }));
    setStep(prev => prev + 1);
  };

  const toggleArrayField = (field, value) => {
    setFormState(prev => {
      const current = new Set(prev[field]);
      current.has(value) ? current.delete(value) : current.add(value);
      return { ...prev, [field]: [...current] };
    });
  };

  const canSubmit =
    formState.name &&
    formState.equipment &&
    formState.optimize.length &&
    formState.playStyle &&
    formState.frequency;

  const handleSubmit = async () => {
    setLoading(true);
    try {
      const payload = {
        //user_id: formState.name,
        equipment_type: formState.equipment,
        preferred_brand: formState.brands,
        aspects_to_optimize: formState.optimize,
        skill_level: formState.playStyle,
        play_frequency: formState.frequency
      };
      console.log("Sending payload to backend:", payload);

      const response = await fetch("http://localhost:5001/api/user-profile", {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify(payload)
      });

      if (!response.ok) {
        const errorText = await response.text();
        console.error(`Failed to save user profile: ${response.status} - ${errorText}`);
        throw new Error(`HTTP ${response.status}`);
      }

      const result = await response.json();
      console.log("Received result from backend:", result);
      if (result?.validation?.result === "VALID") {
        console.log("Successfully created UserProfile in Foundry:", result);
        navigate("/results", { state: { formState } }); //holding inputs for page 2
      } else {
        console.error("Foundry response indicates validation failure:", result);
        alert("Error: Submission was not valid. Check console for details.");
      }
    } catch (err) {
      console.error("Error during submission:", err);
      alert("There was an error submitting your form. Check console.");
      setLoading(false);
    }
  };


  return (
    <div className="page">
      <div className="logo-title">
        <img src="/logo.png" alt="Logo" className="logo-image" />
        <h1 className="title">Personal Pro Shop</h1>
      </div>
      <h2 className="main-header">Welcome to your Personal Pro Shop</h2>
      <p className="sub-header">Opening Up the World of Tennis</p>

      {loading ? (
        <div className="loader">Loading...</div>
      ) : (
        <>
          {step >= 1 && (
            <div className="question">
              <label>What is your name?</label>
              <input
                type="text"
                value={formState.name}
                onChange={e => handleChange("name", e.target.value)}
              />
            </div>
          )}

          {step >= 2 && (
            <div className="question">
              <label>What equipment are you looking for?</label>
              <div className="horizontal-radio-group">
                {["racket", "strings", "racket & strings"].map(opt => (
                  <label key={opt} className="horizontal-option">
                    <input
                      type="radio"
                      id={opt}
                      name="equipment"
                      value={opt}
                      onChange={e => handleChange("equipment", e.target.value)}
                    />
                    {opt}
                  </label>
                ))}
              </div>
            </div>
          )}

          {(formState.equipment === "racket" ||
            formState.equipment === "racket & strings") &&
            step >= 3 && (
              <div className="question">
                <label>Do you have a preferred brand for your racket?</label>
                <div className="horizontal-options">
                  {[
                    "head",
                    "wilson",
                    "yonex",
                    "babolat",
                    "volkl",
                    "prokennex",
                    "prince",
                    "dunlop",
                    "tecnifibre"
                  ].map(brand => (
                    <div key={brand}>
                      <input
                        type="checkbox"
                        id={brand}
                        onChange={() => toggleArrayField("brands", brand)}
                      />
                      <label htmlFor={brand}>{brand}</label>
                    </div>
                  ))}
                </div>
              </div>
          )}

          {step >= 4 && (
            <div className="question">
              <label>What elements of your game/equipment are you looking to optimize?</label>
              <small>(You can select up to 3)</small>
              <div className="horizontal-options">
                {["Power", "Control", "Spin", "Durability"].map(item => (
                  <div key={item}>
                    <input
                      type="checkbox"
                      id={item}
                      onChange={() => {
                        if (formState.optimize.includes(item)) {
                          toggleArrayField("optimize", item);
                        } else if (formState.optimize.length < 3) {
                          toggleArrayField("optimize", item);
                        }
                      }}
                    />
                    <label htmlFor={item}>{item}</label>
                  </div>
                ))}
              </div>
            </div>
          )}

          {step >= 5 && (
            <div className="question">
              <label>What is your preferred play style?</label>
              <div className="horizontal-options">
                {["baseline", "net-court", "all-court"].map(style => (
                  <div key={style}>
                    <input
                      type="radio"
                      id={style}
                      name="playStyle"
                      value={style}
                      onChange={e => handleChange("playStyle", e.target.value)}
                    />
                    <label htmlFor={style}>{style}</label>
                  </div>
                ))}
              </div>
            </div>
          )}

          {step >= 6 && (
            <div className="question">
              <label>How many times a week do you play?</label>
              <div className="horizontal-options">
                {[1, 2, 3, 4, 5, 6, 7].map(n => (
                  <div key={n}>
                    <input
                      type="radio"
                      id={`freq-${n}`}
                      name="frequency"
                      value={n}
                      onChange={e => handleChange("frequency", parseInt(e.target.value, 10))}
                    />
                    <label htmlFor={`freq-${n}`}>{n}</label>
                  </div>
                ))}
              </div>
            </div>
          )}

          {canSubmit && (
            <button className="submit-button" onClick={handleSubmit}>
              Get Recommendation
            </button>
          )}
        </>
      )}
    </div>
  );
};

export default PageOne;
