import './globals.css';

export const metadata = {
  title: 'Null Pointers Chatbot',
  description: 'A dark-themed chatbot interface for your backend',
};

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
