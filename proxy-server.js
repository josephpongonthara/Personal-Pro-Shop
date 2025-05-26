const express = require('express');
const fetch = require('node-fetch');
const dotenv = require('dotenv');
dotenv.config();
const cors = require('cors'); 
const app = express();
app.use(cors()); 
const PORT = 5001;

app.use(express.json());

//USER PROFILE API POST CALL
app.post('/api/user-profile', async (req, res) => {
  console.log('Received request body from frontend:', req.body);

  try {
    const response = await fetch(
      'https://josephpongonthara.usw-16.palantirfoundry.com/api/v2/ontologies/ontology-4a8e99cc-e8e4-4fea-9a4a-99c9bebe8fac/actions/create-user-profile/apply',
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${process.env.FOUNDRY_TOKEN}`, 
        },
        body: JSON.stringify({
          parameters: req.body, 
        }),
      }
    );

    let data;
    try {
        console.log('Palantir status:', response.status);
        data = await response.json(); // Try parsing JSON
    } catch (jsonErr) {
      const text = await response.text(); // fallback
      console.error('Failed to parse JSON. Raw response:', text);
      return res.status(500).json({ error: 'Invalid JSON response from Palantir backend' });
    }

    res.json(data);
  } catch (err) {
    console.error('Proxy error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

//USER PROFILE LIST OBJECTS GET CALL (note user-profile vs user-profiles)
app.get('/api/user-profiles', async (req, res) => {
  try {
    const response = await fetch(
      'https://josephpongonthara.usw-16.palantirfoundry.com/api/v2/ontologies/ontology-4a8e99cc-e8e4-4fea-9a4a-99c9bebe8fac/objects/UserProfile',
      {
        method: 'GET',
        headers: {
          'Authorization': `Bearer ${process.env.FOUNDRY_TOKEN}`,
        }
      }
    );

    if (!response.ok) {
      const errorText = await response.text();
      console.error(`GET failed with status ${response.status}: ${errorText}`);
      return res.status(response.status).json({ error: errorText });
    }

    const data = await response.json();
    console.log('Fetched user profiles from Foundry:', data);
    res.json(data);
  } catch (err) {
    console.error('Error fetching user profiles:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

//RECOMMENDATION RESULT LIST OBJECTS GET CALL (note user-profile vs user-profiles)
app.get('/api/results', async (req, res) => {
  try {
    const response = await fetch(
      'https://josephpongonthara.usw-16.palantirfoundry.com/api/v2/ontologies/ontology-4a8e99cc-e8e4-4fea-9a4a-99c9bebe8fac/objects/RecommendationResult',
      {
        method: 'GET',
        headers: {
          'Authorization': `Bearer ${process.env.FOUNDRY_TOKEN}`,
        }
      }
    );

    if (!response.ok) {
      const errorText = await response.text();
      console.error(`GET failed with status ${response.status}: ${errorText}`);
      return res.status(response.status).json({ error: errorText });
    }

    const data = await response.json();
    console.log('Fetched user profiles from Foundry:', data);
    res.json(data);
  } catch (err) {
    console.error('Error fetching user profiles:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

//TERMINAL OUTPUT FOR DEBUGGING
app.listen(PORT, () => {
  console.log(`Proxy server running on port ${PORT}`);
});