# SQLfromScratch-

A lightweight SQL engine built from scratch in C++ that parses and executes basic SQL commands. This project demonstrates fundamental database operations including table creation, data insertion, and querying, all without relying on external database systems.

## 📌 Features

* Custom SQL parser for interpreting user-defined queries
* Support for basic SQL operations: `CREATE TABLE`, `INSERT INTO`, `SELECT`
* In-memory data storage using C++ data structures
* Command-line interface for interactive query execution
* Modular codebase facilitating easy extension and maintenance

## 🛠️ Technologies Used

* **C++**: Core language for implementing the SQL engine
* **JavaScript, CSS, HTML**: For any associated frontend components or interfaces([GitHub][1])

## 📁 Project Structure

```
SQLfromScratch-/
├── src/
│   ├── main.cpp          # Entry point of the application
│   ├── parser.cpp        # SQL query parser implementation
│   ├── executor.cpp      # Executes parsed SQL commands
│   └── utils.cpp         # Utility functions and helpers
├── include/
│   ├── parser.h          # Header for parser
│   ├── executor.h        # Header for executor
│   └── utils.h           # Header for utilities
├── data/
│   └── sample_data.txt   # Sample data files (if any)
├── README.md             # Project documentation
└── Makefile              # Build configuration
```



## 🚀 Installation

### Prerequisites

* C++ compiler (e.g., `g++`)
* Make utility

### Steps

1. **Clone the Repository**

   ```bash
   git clone https://github.com/MobeenButt/SQLfromScratch-.git
   cd SQLfromScratch-
   ```



2. **Build the Project**

   ```bash
   make
   ```



This will compile the source files and generate an executable named `sql_engine`.

3. **Run the Application**

   ```bash
   ./sql_engine
   ```



Upon running, you'll be presented with a prompt to enter SQL commands.

## 🧪 Usage

Once the application is running, you can input SQL commands. Here are some examples:

* **Create a Table**

```sql
  CREATE TABLE students (id INT, name TEXT, age INT);
```



* **Insert Data**

```sql
  INSERT INTO students VALUES (1, 'Alice', 20);
  INSERT INTO students VALUES (2, 'Bob', 22);
```



* **Select Data**

```sql
  SELECT * FROM students;
```



The engine will parse and execute these commands, displaying results or confirmations as appropriate.

## 👥 Contributors

* [Mobeen Butt](https://github.com/MobeenButt)
* [Muhammad Talha](https://github.com/mtalha1501)
* [Anas Faiz](https://github.com/anassaahi)

## 📄 License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## 📬 Contact

For questions or suggestions, feel free to reach out to [mobeen914butt@gmail.com](mailto:mobeen914butt@gmail.com).([GitHub][1])

---

*Note: This README is based on the available information from the [SQLfromScratch- GitHub repository](https://github.com/MobeenButt/SQLfromScratch-). For the most accurate and detailed information, please refer to the repository directly.*

---

[1]: https://github.com/MobeenButt?utm_source=chatgpt.com "Mobeen Butt MobeenButt - GitHub"
