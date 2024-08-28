<h1>Sales and Warehouse Live Data Dashboard 📊</h1>

<img src="./diagrams/dashboard.png" alt="Dashboard Diagram">

<p>
This project is designed to create a live data dashboard for Sales and Warehouse using a modern data streaming and processing architecture. The architecture leverages Apache Kafka for data streaming, Apache Spark for processing, Cassandra for data storage, and Plotly Dash for creating an analytical dashboard. All components are containerized using Docker to ensure easy deployment and scalability.
</p>

<h2>Architecture Overview 🔧</h2>

<ul>
    <li><strong>🐳 Docker:</strong> All components are containerized to ensure consistent environments across different platforms and ease of deployment.</li>
    <li><strong>🐍 Python Script (Producer):</strong> Acts as the data source, sending data streams to specific Kafka topics ("Sales" and "Warehouse").</li>
    <<li><strong>📦 Kafka Cluster:</strong> Comprises three controllers and three brokers to manage and distribute the data streams. This setup uses Kraft (Kafka Raft) mode instead of the traditional Zookeeper-based architecture. Kraft eliminates the need for Zookeeper by integrating consensus and metadata management directly into Kafka. This results in a simpler architecture with reduced operational complexity, improved scalability, and faster recovery from failures. Kafka automatically creates the required topics (init topics container) and ensures data flows correctly through the system.</li>
    <li><strong>⚡ Apache Spark (Consumer):</strong> Receives and processes the streaming data from Kafka. The Spark cluster consists of two workers and one master node, where Spark submits jobs (on the workers) one after the other, processes the data based on the Kafka topics ("Sales" and "Warehouse"), and inserts the results into Cassandra, acting as a consumer.</li>
    <li><strong>📊 Cassandra:</strong> Stores the processed data from Spark. It offers high availability and scalability, making it ideal for real-time data storage.</li>
    <li><strong>📈 Plotly Dash:</strong> Provides an analytical dashboard for visualizing the data stored in Cassandra, allowing users to interact with and analyze the live data streams. It allows us to switch between Sales and Warehouse live data.</li>
    <li><strong>🚀 init-cassandra Container:</strong> This additional container automatically creates the keyspace and the necessary tables in Cassandra when the environment is started, ensuring full automation of the setup process.</li>
</ul>

<h2>Getting Started</h2>

<h3>Prerequisites</h3>
<p>🐳 Docker and Docker Compose installed on your machine</p>

<h3>Setting Up the Environment</h3>

<li><strong>Clone the repository:</strong>
        <pre><code>git clone git@github.com:NorthBrains/dashboard-1.git</code></pre>
</li>

<li><strong>Start the Docker containers:</strong>
        <pre><code>docker-compose up -d</code></pre>
        <p>Once Docker Compose is up, all services including the streaming, processing, and dashboard will automatically start without requiring additional configuration.</p>
</li>

<li><strong>Set Up Cassandra Keyspace and Tables:</strong>
        <p>After the Cassandra container is up and running, the <strong>init-cassandra</strong> container will automatically create the necessary keyspace and tables. This container ensures that the database schema is initialized properly without manual intervention.</p>
</li>

<h3>Checking if the Streaming is Working</h3>
<p>To verify if the streaming is working correctly, you can execute the following commands to query the data in Cassandra:</p>
<pre><code>docker exec -it cassandra_one cqlsh -u cassandra -p cassandra</code></pre>
<pre><code>SELECT * FROM company_one.sales_data;</code></pre>
<pre><code>SELECT * FROM company_one.warehouse_data;</code></pre>

<h3>Accessing the Spark Master GUI</h3>
<p>You can access the Spark Master GUI by navigating to <a href="http://localhost:8190">http://localhost:8190</a> in your web browser.</p>
<p>This interface allows you to monitor the status of running Spark workers and applications. You can check the health and performance of the Spark cluster, including the details of each worker node, active jobs, stages, and tasks.</p>

<h3>Accessing the Dashboard</h3>
<p>Once all services are up and running, you can access the Plotly Dash dashboard by navigating to <a href="http://localhost:8900">http://localhost:8900</a> in your web browser.</p>

<h3>Stopping the Environment</h3>
<p>To stop and remove all running containers, execute:</p>
<pre><code>docker-compose down</code></pre>

<h2>Additional Information</h2>

<ul>
    <li><strong>📦 Kafka:</strong> Ensure that the topics are correctly initialized and the data is being streamed to the appropriate topics (<code>Sales</code> and <code>Warehouse</code>).</li>
    <li><strong>⚡ Spark:</strong> The Spark jobs should be configured to read from Kafka, process the data, and write the results to Cassandra.</li>
    <li><strong>📊 Cassandra:</strong> Regularly monitor the storage and performance to ensure that it scales according to the incoming data volume.</li>
    <li><strong>📈 Dash:</strong> Customize the dashboards as needed to include more visualizations or interactive elements that suit your data analysis needs.</li>
</ul>

<h2>Contributions</h2>
<p>Feel free to contribute to this project by submitting issues or pull requests. All contributions are welcome and appreciated!</p>
