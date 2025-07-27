import React from 'react';
import { render, screen } from '@testing-library/react';
import App from './App';

test('renders SQS Admin', () => {
  render(<App />);
  const titleElement = screen.getByText(/SQS Admin/i);
  expect(titleElement).toBeInTheDocument();
});
