# radarbase

The inspiration for Radarbase comes from a unique real-world challenge its creator experienced while working part-time at a local financial firm. Tasked with the development of a web application called "Radar", which visualized a plethora of worldwide stock data via radar charts, the developer confronted the issues arising from heavy client-side data processing.

The initial "Radar" application was a client-side application that processed and presented all data in the browser using a CSV format. As the amount of stock data increased, so did the complexity of the front-end code and the application's response time, leading to an inefficient system.

Around this time, the developer was studying distributed data systems, delving into works like the Google File System paper. This led to the insight that a durable storage system could simplify the design of an application by holding persistent state remotely. This insight sparked a two-week long journey to develop a backend for the "Radar" project that could handle large amounts of stock data efficiently and reliably. The successful outcome transformed the "Radar" project, reducing front-end development time and shifting the focus towards enhancing user interaction.

This journey served as a powerful learning experience, emphasizing the importance of proper **documentation**, comprehensive **unit tests**, and solid **data structures**. As a result, the idea for Radarbase was born, a project aiming to encapsulate these lessons into a robust, reliable, and efficient key-value store system.

While the name "Radarbase" is a nod to the backend developed for the "Radar" project, it's important to clarify that this Radarbase project aims to go beyond the original use-case. This new Radarbase is designed as a general-purpose key-value store, built to provide high reliability and exceptional performance for a wide range of applications. It is a standalone project, inspired by the past but created for the future, with a vision to be a versatile tool in the world of data management.
