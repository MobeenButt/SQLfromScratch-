# MY_DATABASE

A database management system implementation as part of an academic project.

## Overview

MY_DATABASE is a simple database management system built using Node.js and Express. This project demonstrates fundamental database operations including creating tables, inserting records, and querying data through a REST API interface.

## Features

- User authentication system
- Table creation and management
- Data insertion and retrieval
- RESTful API endpoints
- Basic frontend interface for database operations

## Technologies Used

- Node.js
- Express.js
- MongoDB
- HTML/CSS/JavaScript
- Postman (for API testing)

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/MobeenButt/MY_DATABASE.git
   ```

2. Navigate to the project directory:
   ```
   cd MY_DATABASE
   ```

3. Install dependencies:
   ```
   npm install
   ```

4. Configure environment variables:
   - Create a `.env` file in the root directory
   - Add the following variables:
     ```
     PORT=3000
     MONGODB_URI=mongodb://localhost:27017/mydatabase
     JWT_SECRET=yoursecretkey
     ```

5. Start the server:
   ```
   npm start
   ```

## Usage

### API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | /api/auth/register | Register a new user |
| POST | /api/auth/login | Login and get auth token |
| GET | /api/tables | Get all tables |
| POST | /api/tables | Create a new table |
| GET | /api/tables/:id | Get table by ID |
| PUT | /api/tables/:id | Update a table |
| DELETE | /api/tables/:id | Delete a table |
| POST | /api/query | Execute a database query |

### Example API Requests

#### Create a new table
```json
POST /api/tables
{
  "tableName": "users",
  "columns": [
    {
      "name": "id",
      "type": "INTEGER",
      "primaryKey": true
    },
    {
      "name": "username",
      "type": "TEXT",
      "notNull": true
    },
    {
      "name": "email",
      "type": "TEXT",
      "unique": true
    }
  ]
}
```

#### Execute a query
```json
POST /api/query
{
  "query": "SELECT * FROM users WHERE id = 1"
}
```

## Project Structure

```
MY_DATABASE/
├── config/
│   └── db.js
├── controllers/
│   ├── authController.js
│   ├── tableController.js
│   └── queryController.js
├── middleware/
│   └── auth.js
├── models/
│   ├── User.js
│   └── Table.js
├── routes/
│   ├── auth.js
│   ├── tables.js
│   └── query.js
├── public/
│   ├── css/
│   ├── js/
│   └── index.html
├── .env
├── package.json
├── server.js
└── README.md
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contact

Mobeen Butt - mobeen914butt@gmail.com
Anas Faiz - anasfaizsahi6@gmail.com
Project Link: [https://github.com/MobeenButt/MY_DATABASE](https://github.com/MobeenButt/MY_DATABASE)

## Acknowledgements

- [Node.js](https://nodejs.org/)
- [Express.js](https://expressjs.com/)
- [MongoDB](https://www.mongodb.com/)
