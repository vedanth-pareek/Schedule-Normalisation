import os
from flask import Flask, request, redirect, url_for, render_template, send_file, jsonify
from werkzeug.utils import secure_filename
import pandas as pd
import pymysql
from openpyxl import load_workbook
import io
from datetime import datetime

app = Flask(__name__, template_folder='my_templates')
app.config['ALLOWED_EXTENSIONS'] = {'csv', 'xlsx'}

app.config['UPLOAD_FOLDER'] = os.path.join(os.getcwd(), 'uploads')
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

def get_db_connection():
    return pymysql.connect(
        host="localhost",
        user="root",
        password="rootuser",
        database="files_db"
    )

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']

def is_valid_csv(file):
    try:
        pd.read_csv(file)
        file.seek(0)
        return True
    except Exception:
        return False

def is_valid_xlsx(file):
    try:
        file.seek(0)
        load_workbook(file)
        return True
    except Exception as e:
        print(f"Error loading XLSX file: {e}")
        return False

def create_metadata_table():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS metadata (
            id INT AUTO_INCREMENT PRIMARY KEY,
            timezone VARCHAR(255),
            customer VARCHAR(255),
            channel_identifier VARCHAR(255),
            EPGID VARCHAR(255) UNIQUE,
            genre VARCHAR(255),
            startTime VARCHAR(255),
            endTime VARCHAR(255),
            duration VARCHAR(255),
            Date VARCHAR(255),
            rating VARCHAR(255),
            program VARCHAR(255),
            description VARCHAR(255),
            provider VARCHAR(255),
            skiprow VARCHAR(255),
            default_duration VARCHAR(255),
            thumbnail_image VARCHAR(255),
            sheet_number VARCHAR(255),
            episode VARCHAR(255)
        );
    """)
    conn.commit()
    cursor.close()
    conn.close()

def load_metadata_to_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    metadata_file = 'Metadata_EPG.csv'
    df_metadata = pd.read_csv(metadata_file)
    
    df_metadata.dropna(how='all', inplace=True)
    df_metadata.fillna(value='-', inplace=True)
    
    cursor.execute("TRUNCATE TABLE `metadata`")

    for _, row in df_metadata.iterrows():
        try:
            cursor.execute("""
                INSERT INTO `metadata` (
                    `timezone`, `customer`, `channel_identifier`, `EPGID`, `genre`,
                    `startTime`, `endTime`, `duration`, `Date`, `rating`, 
                    `program`, `description`, `provider`, `skiprow`, `default_duration`, 
                    `thumbnail_image`, `sheet_number`, `episode`
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['timezone'], row['customer'], row['channel_identifier'], row['EPGID'], row['genre'],
                row['startTime'], row['endTime'], row['duration'], row['Date'], row['rating'], 
                row['program'], row['description'], row['provider'], row['skiprow'], row['default_duration'], 
                row['thumbnail_image'], row['sheet_number'], row['episode']
            ))
        except Exception as e:
            print(f"Error inserting row: {row}")
            print(f"Error details: {e}") 
    
    conn.commit()
    cursor.close()
    conn.close()

def load_metadata():
    conn = get_db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("SELECT * FROM metadata")
    metadata = cursor.fetchall()
    cursor.close()
    conn.close()
    return metadata

def map_columns(EPGID, df):
    conn = get_db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("SELECT * FROM metadata WHERE EPGID=%s", (EPGID,))
    mapping = cursor.fetchone()
    cursor.close()
    conn.close()
    if not mapping:
        return df
    column_mapping = {v: k for k, v in mapping.items() if k not in ['id', 'EPGID']}
    return df.rename(columns=column_mapping)

@app.route('/')
def index():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT DISTINCT `EPGID` FROM `metadata`")
    channels = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("SHOW TABLES")
    files = [table[0] for table in cursor.fetchall()]
    
    cursor.close()
    conn.close()
    processed_files = [f for f in os.listdir(app.config['UPLOAD_FOLDER']) if f.endswith('.csv')]
    return render_template('portal.html', files=processed_files, channels=channels)

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"error": "No file part"})
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file"})
    
    channel = request.form.get('EPGID')

    if channel == 'new':
        def get_field_value(field_name):
            return request.form.get(field_name, '').strip() or '-'
    
        timezone = get_field_value('meta_col_1')
        customer = get_field_value('meta_col_2')
        channel_identifier = get_field_value('meta_col_3')
        EPGID = get_field_value('meta_col_4')
        genre = get_field_value('meta_col_5')
        startTime = get_field_value('meta_col_6')
        endTime = get_field_value('meta_col_7')
        duration = get_field_value('meta_col_8')
        Date = get_field_value('meta_col_9')
        rating = get_field_value('meta_col_10')
        program = get_field_value('meta_col_11')
        description = get_field_value('meta_col_12')
        provider = get_field_value('meta_col_13')
        skiprow = get_field_value('meta_col_14')
        default_duration = get_field_value('meta_col_15')
        thumbnail_image = get_field_value('meta_col_16')
        sheet_number = get_field_value('meta_col_17')
        episode = get_field_value('meta_col_18')
        
        metadata_file = 'Metadata_EPG.csv'
        df_metadata = pd.read_csv(metadata_file)
        
        new_row = {
            'timezone': timezone,
            'customer': customer,
            'channel_identifier': channel_identifier,
            'EPGID': EPGID,
            'genre': genre,
            'startTime': startTime,
            'endTime': endTime,
            'duration': duration,
            'Date': Date,
            'rating': rating,
            'program': program,
            'description': description,
            'provider': provider,
            'skiprow': skiprow,
            'default_duration': default_duration,
            'thumbnail_image': thumbnail_image,
            'sheet_number': sheet_number,
            'episode': episode
        }
        df_metadata = df_metadata._append(new_row, ignore_index=True)
        
        df_metadata.to_csv(metadata_file, index=False)
        
        conn = get_db_connection()
        cursor = conn.cursor()
    
        insert_query = """
            INSERT INTO metadata (timezone, customer, channel_identifier, EPGID, genre, startTime, 
                                  endTime, duration, Date, rating, program, description, provider, 
                                  skiprow, default_duration, thumbnail_image, sheet_number, episode)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
    
        cursor.execute(insert_query, (timezone, customer, channel_identifier, EPGID, genre, startTime, 
                                      endTime, duration, Date, rating, program, description, provider, 
                                      skiprow, default_duration, thumbnail_image, sheet_number, episode))
    
        conn.commit()
        cursor.execute("SELECT * FROM metadata WHERE EPGID = %s", (EPGID,))
        new_channel_metadata = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if new_channel_metadata:
            column_mapping = {v: k for k, v in new_channel_metadata.items() if k not in ['id', 'channel_identifier', 'EPGID']}
            df.rename(columns=column_mapping, inplace=True)
        
    if file and allowed_file(file.filename):
        base_filename = secure_filename(file.filename)
        filename_without_ext = os.path.splitext(base_filename)[0]
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        processed_filename = f"{filename_without_ext}_{channel}_{timestamp}.csv"
        processed_filepath = os.path.join(app.config['UPLOAD_FOLDER'], processed_filename)
        
        if base_filename.endswith('.csv'):
            if not is_valid_csv(file):
                return jsonify({"error": "Invalid CSV file"})
            df = pd.read_csv(file)
        elif base_filename.endswith('.xlsx'):
            if not is_valid_xlsx(file):
                return jsonify({"error": "Invalid XLSX file"})
            df = pd.read_excel(file)
        
        df.columns = df.columns.str.strip()
        df.columns = [col if col else '-' for col in df.columns]
        df = df.loc[:, df.columns != '-']
        df = map_columns(channel, df)
        
        table_name = filename_without_ext
        
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
        cursor.execute(f"CREATE TABLE `{table_name}` (id INT AUTO_INCREMENT PRIMARY KEY)")
        
        schema_mapping = {}
        if channel != 'new':
            cursor.execute(
                """
                SELECT EPGID, genre, startTime, endTime, duration, Date, rating, program, 
                       description, provider, skiprow, default_duration, thumbnail_image, 
                       sheet_number, episode
                FROM metadata 
                WHERE EPGID = %s
                """, 
                (channel,)
            )
        
        result = cursor.fetchone()
    
        if result:
            schema_mapping = {
                'EPGID': result[0],
                'genre': result[1],
                'startTime': result[2],
                'endTime': result[3],
                'duration': result[4],
                'Date': result[5],
                'rating': result[6],
                'program': result[7],
                'description': result[8],
                'provider': result[9],
                'skiprow': result[10],
                'default_duration': result[11],
                'thumbnail_image': result[12],
                'sheet_number': result[13],
                'episode': result[14],
            }

        valid_mapping = {k: v for k, v in schema_mapping.items()if v in df.columns}       
        df.rename(columns = valid_mapping, inplace=True)
        print("Renamed Columns: ", df.columns)        

        for column in df.columns:
            if not column.strip():
                continue
            column_escaped = column.replace("`", "``")
            max_length = df[column].astype(str).map(len).max()
            column_type = "TEXT" if max_length > 255 else "VARCHAR(255)"
            cursor.execute(f"ALTER TABLE `{table_name}` ADD COLUMN `{column_escaped}` {column_type}")
        
        insert_query = f"INSERT INTO `{table_name}` ({', '.join([f'`{col.replace('`', '``')}`' for col in df.columns])}) VALUES ({', '.join(['%s'] * len(df.columns))})"
        for _, row in df.iterrows():
            cursor.execute(insert_query, tuple(row[col] for col in df.columns))
        
        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({"success": True, "filename": processed_filename})
    
    return jsonify({"error": "File not allowed"})

@app.route('/download/<filename>', methods=['GET'])
def download_file(filename):
    file_format = request.args.get('format')
    table_name = filename
    
    conn = get_db_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(f"SELECT * FROM `{table_name}`")
    rows = cursor.fetchall()
    df = pd.DataFrame(rows)
    
    output = io.BytesIO()
    if file_format == 'csv':
        df.to_csv(output, index=False)
        output.seek(0)
        return send_file(output, as_attachment=True, download_name=f"{table_name}.csv")
    elif file_format == 'xlsx':
        df.to_excel(output, index=False)
        output.seek(0)
        return send_file(output, as_attachment=True, download_name=f"{table_name}.xlsx")
    elif file_format == 'json':
        df.to_json(output, orient='records')
        output.seek(0)
        return send_file(output, as_attachment=True, download_name=f"{table_name}.json")
    
    cursor.close()
    conn.close()

if __name__ == '__main__':
    load_metadata_to_db()
    app.run(debug=True)
