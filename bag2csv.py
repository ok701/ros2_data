import sqlite3
import csv
from rosidl_runtime_py import message_to_ordereddict
from rclpy.serialization import deserialize_message
from rosidl_runtime_py.utilities import get_message

def bag_to_csv(bag_file):
    conn = sqlite3.connect(bag_file)
    cursor = conn.cursor()

    # Get all topics
    cursor.execute("SELECT name, type FROM topics")
    topics = cursor.fetchall()

    for topic_name, topic_type in topics:
        msg_type = get_message(topic_type)
        
        # Get messages
        cursor.execute("SELECT data FROM messages WHERE topic_id = (SELECT id FROM topics WHERE name = ?)", (topic_name,))
        messages = cursor.fetchall()

        # Create output file name based on topic name
        output_file = f"{topic_name.replace('/', '_')}.csv"

        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            
            if messages:
                # Write header
                first_msg = deserialize_message(messages[0][0], msg_type)
                header = message_to_ordereddict(first_msg).keys()
                writer.writerow(header)

                # Write data
                for msg in messages:
                    deserialized_msg = deserialize_message(msg[0], msg_type)
                    row = message_to_ordereddict(deserialized_msg).values()
                    writer.writerow(row)

    conn.close()

bag_to_csv('/home/awear/ros2_data/all_topics/all_topics_0.db3')