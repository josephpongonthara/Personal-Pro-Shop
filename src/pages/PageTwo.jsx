import React, { useEffect, useState } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import '../App.css';

const PageTwo = () => {
  const location = useLocation();
  const navigate = useNavigate();
  const formState = location.state?.formState;

  const [userId, setUserId] = useState(null);
  const [recommendationData, setRecommendationData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [matchError, setMatchError] = useState(null);

  const handleReset = () => {
    navigate('/');
  };

  const normalize = (value) =>
    typeof value === 'string' ? value.trim().toLowerCase() : value;

  useEffect(() => {
    const fetchUserProfilesAndMatch = async () => {
      try {
        // 1. Fetch all user profiles
        const userRes = await fetch('http://localhost:5001/api/user-profiles');
        if (!userRes.ok) {
          const errorText = await userRes.text();
          throw new Error(`UserProfile to fetch: ${userRes.status} - ${errorText}`);
        }

        const userData = await userRes.json();
        const users = userData.data;

        console.log('Fetched UserProfiles:', users);
        console.log('FormState:', formState);

        const arraysEqualIgnoreOrder = (a, b) => {
          if (!Array.isArray(a) || !Array.isArray(b)) return false;
          if (a.length !== b.length) return false;
          return [...a].sort().every((val, idx) => val === [...b].sort()[idx]);
        };        

        //match formstate to a user profile
        const matchedUser = users.find(user => {
          const brandMatch = arraysEqualIgnoreOrder(formState.brands, user.preferredBrand);
          const aspectMatch = arraysEqualIgnoreOrder(formState.optimize, user.aspectsToOptimize);

          const matchConditions = {
            equipmentType: normalize(user.equipmentType) === normalize(formState.equipment),
            brandMatch,
            aspectMatch,
            skillLevel: normalize(user.skillLevel) === normalize(formState.playStyle),
            playFrequency: String(user.playFrequency) === String(formState.frequency)
            };

            console.log('Checking user:', user);
            console.log('Match conditions:', matchConditions);
      
          return Object.values(matchConditions).every(Boolean);
        });

        if (!matchedUser){
          setMatchError('No matching user profile found.');
          setLoading(false);
          return;
        }
        
        console.log('Matched user:', matchedUser);
        const userId = matchedUser?.userId;
        console.log('Looking for recommendations linked to userId:', userId);

        const MAX_ATTEMPTS = 18; // waiting for 1.5 mins
        const DELAY_MS = 5000;

        const wait = (ms) => new Promise(resolve => setTimeout(resolve, ms));

        let attempts = 0;
        let matchedResult = null;

        while (attempts < MAX_ATTEMPTS && !matchedResult) {
          console.log(`Polling attempt ${attempts + 1} for recommendations...`);

          const recRes = await fetch('http://localhost:5001/api/results');
          if (!recRes.ok) {
            const errorText = await recRes.text();
            throw new Error(`RecommendationResult fetch failed: ${recRes.status} - ${errorText}`);
          }

          const recData = await recRes.json();
          const results = recData.data;

          console.log('Results received:', results);

          matchedResult = results.find(result => result.linkedUser === userId);

          if (!matchedResult) {
            attempts++;
            if (attempts < MAX_ATTEMPTS) {
              await wait(DELAY_MS);
            }
          }
        }

        if (!matchedResult) {
          setMatchError('No matching recommendation found.');
          setLoading(false);
          return;
        }
        console.log('Matched recommendation result:', matchedResult);
        //storing matched recommendation
        setRecommendationData({
          racketId: matchedResult.racketId,
          stringId: matchedResult.stringId,
          llmExplanation: matchedResult.llmExplanation,
        });
      } catch (err) {
        console.error('Error fetching or matching data:', err);
        setMatchError(err.message);
      } finally {
        setLoading(false);
      }
    };
    if (formState) {
      fetchUserProfilesAndMatch();
    } else {
      setMatchError('No form data available.');
      setLoading(false);
    }
  }, [formState]);

// LOADING & CHECKING FOR ERRORS   
  if (loading) {
    return (
      <div className="page">
        <div className="logo-title">
          <img src="/logo.png" alt="Logo" className="logo-image" />
          <h1 className="title">Personal Pro Shop</h1>
        </div>
        <div className="loader">
          {formState?.name
            ? `Hang tight, ${formState.name} — we're building your personalized recommendations...`
            : "Loading user profile..."}
        </div>
      </div>
    );
  }
  if (matchError) {
    return (
      <div className="page">
        <div className="logo-title">
          <img src="/logo.png" alt="Logo" className="logo-image" />
          <h1 className="title">Personal Pro Shop</h1>
        </div>
        <div className="error">Error: {matchError}</div>
        <button className="submit-button" onClick={handleReset}>
        Start Over
        </button>
      </div>
    );
  }
  //MAIN RENDER
  return (
    <div className="page">
      {/* LOGO & TITLE (repeat from PageOne) */}
      <div className="logo-title">
        <img src="/logo.png" alt="Logo" className="logo-image" />
        <h1 className="title">Personal Pro Shop</h1>
      </div>
  
      <h2 className="main-header">Your Personalized Recommendations</h2>
  
      <div className="recommendation spaced">
        {/* Racket */}
        {Array.isArray(recommendationData.racketId) &&
          !recommendationData.racketId.includes("This equipment type was not requested") && (
            <div className="equipment-block">
              <strong>Racket Recommendations:</strong>
              {recommendationData.racketId.map((racket, index) => (
                <p key={index}>{racket.trim()}</p>
              ))}
            </div>
          )}

          {/* String */}
          {Array.isArray(recommendationData.stringId) &&
            !recommendationData.stringId.includes("This equipment type was not requested") && (
              <div className="equipment-block">
                <strong>String Recommendations:</strong>
                {recommendationData.stringId.map((string, index) => (
                  <p key={index}>{string.trim()}</p>
                ))}
              </div>
            )}
  
          {/* LLM Explanation */}
          {Array.isArray(recommendationData.llmExplanation) && (
            <div className="equipment-block">
              <strong>Recommendations Explained:</strong>
              {recommendationData.llmExplanation.map((explanation, index) => (
                <p key={index}>{explanation.trim()}</p>
              ))}
            </div>
          )}
      </div>
  
      <button className="submit-button" onClick={handleReset}>
        Start Over
      </button>
    </div>
  );
};

export default PageTwo;
