import React from 'react';
import { Routes, Route } from 'react-router-dom';
import PageOne from './pages/PageOne.jsx';
import PageTwo from './pages/PageTwo.jsx';
import './App.css';

function App() {
  return (
    <Routes>
      <Route path="/" element={<PageOne />} />
      <Route path="/results" element={<PageTwo />} />
    </Routes>
  );
}

export default App;
