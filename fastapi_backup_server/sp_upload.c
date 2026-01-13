#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <curl/curl.h>
#include <sys/stat.h>
#include <dirent.h>
#include <time.h>
#include <unistd.h>

#define MAX_URL_LEN 2048
#define MAX_PATH_LEN 1024
#define MAX_FILES 1000

// Configuration
typedef struct {
    char *server_url;           // e.g., http://spserver:1580
    char *node_id;              // e.g., APPLEBEES
    char *password;
    char *backup_directory;     // e.g., /sp_backups/ceph_downloads
} SPConfig;

// Response buffer
typedef struct {
    char *data;
    size_t size;
} Response;

// File list
typedef struct {
    char **files;
    int count;
} FileList;

// Write callback for API responses
static size_t write_callback(void *contents, size_t size, size_t nmemb, void *userp) {
    size_t realsize = size * nmemb;
    Response *mem = (Response *)userp;
    
    char *ptr = realloc(mem->data, mem->size + realsize + 1);
    if (!ptr) {
        fprintf(stderr, "Memory allocation failed\n");
        return 0;
    }
    
    mem->data = ptr;
    memcpy(&(mem->data[mem->size]), contents, realsize);
    mem->size += realsize;
    mem->data[mem->size] = 0;
    
    return realsize;
}

// Extract JSON value (simple parser for specific keys)
char* extract_json_value(const char *json, const char *key) {
    char search[256];
    snprintf(search, sizeof(search), "\"%s\":", key);
    
    char *start = strstr(json, search);
    if (!start) return NULL;
    
    start += strlen(search);
    while (*start == ' ' || *start == '\n' || *start == '\t') start++;
    
    if (*start == '"') {
        start++;
        char *end = strchr(start, '"');
        if (!end) return NULL;
        
        size_t len = end - start;
        char *value = malloc(len + 1);
        strncpy(value, start, len);
        value[len] = '\0';
        return value;
    } else {
        // Number or boolean
        char *end = start;
        while (*end && *end != ',' && *end != '}' && *end != ']') end++;
        
        size_t len = end - start;
        char *value = malloc(len + 1);
        strncpy(value, start, len);
        value[len] = '\0';
        return value;
    }
}

// Sign on to IBM Storage Protect
char* sp_sign_on(SPConfig *config, char **session_id) {
    CURL *curl;
    CURLcode res;
    Response chunk = {0};
    char *task_id = NULL;
    
    curl = curl_easy_init();
    if (!curl) return NULL;
    
    chunk.data = malloc(1);
    chunk.size = 0;
    
    // Build sign-on URL
    char url[MAX_URL_LEN];
    snprintf(url, sizeof(url), "%s/api/baclient/signon", config->server_url);
    
    // Build JSON payload
    char payload[1024];
    snprintf(payload, sizeof(payload),
             "{\"nodeId\":\"%s\",\"password\":\"%s\"}",
             config->node_id, config->password);
    
    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    headers = curl_slist_append(headers, "Accept: application/json");
    
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payload);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&chunk);
    
    printf("Signing on to IBM Storage Protect...\n");
    printf("Server: %s\n", config->server_url);
    printf("Node: %s\n\n", config->node_id);
    
    res = curl_easy_perform(curl);
    
    if (res == CURLE_OK) {
        long response_code;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);
        
        if (response_code == 200 || response_code == 201) {
            printf("✓ Sign-on successful\n");
            
            // Extract session ID from response
            *session_id = extract_json_value(chunk.data, "sessionId");
            task_id = extract_json_value(chunk.data, "taskId");
            
            if (*session_id) {
                printf("Session ID: %s\n", *session_id);
            }
            if (task_id) {
                printf("Task ID: %s\n", task_id);
            }
        } else {
            fprintf(stderr, "✗ Sign-on failed (HTTP %ld)\n", response_code);
            fprintf(stderr, "Response: %s\n", chunk.data);
        }
    } else {
        fprintf(stderr, "✗ Request failed: %s\n", curl_easy_strerror(res));
    }
    
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    free(chunk.data);
    
    return task_id;
}

// Start backup
char* sp_start_backup(SPConfig *config, const char *session_id, const char *backup_path, 
                      const char *backup_name, FileList *file_list) {
    CURL *curl;
    CURLcode res;
    Response chunk = {0};
    char *task_id = NULL;
    
    curl = curl_easy_init();
    if (!curl) return NULL;
    
    chunk.data = malloc(1);
    chunk.size = 0;
    
    // Build backup URL
    char url[MAX_URL_LEN];
    snprintf(url, sizeof(url), "%s/api/baclient/backup", config->server_url);
    
    // Build JSON payload with file list
    char *payload = malloc(8192);
    int offset = 0;
    
    offset += snprintf(payload + offset, 8192 - offset,
                      "{\"sessionId\":\"%s\","
                      "\"backupName\":\"%s\","
                      "\"backupType\":\"ceph_downloads\","
                      "\"backupPath\":\"%s\"",
                      session_id, backup_name, backup_path);
    
    // Add file list if provided
    if (file_list && file_list->count > 0) {
        offset += snprintf(payload + offset, 8192 - offset, ",\"fileList\":[");
        for (int i = 0; i < file_list->count; i++) {
            offset += snprintf(payload + offset, 8192 - offset, 
                             "\"%s\"%s", file_list->files[i], 
                             (i < file_list->count - 1) ? "," : "");
        }
        offset += snprintf(payload + offset, 8192 - offset, "]");
    }
    
    offset += snprintf(payload + offset, 8192 - offset, "}");
    
    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    headers = curl_slist_append(headers, "Accept: application/json");
    
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payload);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&chunk);
    
    printf("\n%s\n", "======================================================================");
    printf("  STARTING BACKUP: %s\n", backup_name);
    printf("%s\n", "======================================================================");
    printf("Source directory: %s\n", backup_path);
    if (file_list) {
        printf("Files to backup: %d\n", file_list->count);
    }
    
    res = curl_easy_perform(curl);
    
    if (res == CURLE_OK) {
        long response_code;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);
        
        if (response_code == 200 || response_code == 201 || response_code == 202) {
            printf("✓ Backup started successfully\n");
            task_id = extract_json_value(chunk.data, "taskId");
            if (task_id) {
                printf("Backup task ID: %s\n", task_id);
            }
        } else {
            fprintf(stderr, "✗ Backup start failed (HTTP %ld)\n", response_code);
            fprintf(stderr, "Response: %s\n", chunk.data);
        }
    } else {
        fprintf(stderr, "✗ Request failed: %s\n", curl_easy_strerror(res));
    }
    
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    free(payload);
    free(chunk.data);
    
    return task_id;
}

// Get task status
char* sp_get_task_status(SPConfig *config, const char *session_id, const char *task_id) {
    CURL *curl;
    CURLcode res;
    Response chunk = {0};
    char *task_state = NULL;
    
    curl = curl_easy_init();
    if (!curl) return NULL;
    
    chunk.data = malloc(1);
    chunk.size = 0;
    
    char url[MAX_URL_LEN];
    snprintf(url, sizeof(url), "%s/api/baclient/task/%s/status", 
             config->server_url, task_id);
    
    char session_header[512];
    snprintf(session_header, sizeof(session_header), "X-Session-Id: %s", session_id);
    
    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, "Accept: application/json");
    headers = curl_slist_append(headers, session_header);
    
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&chunk);
    
    res = curl_easy_perform(curl);
    
    if (res == CURLE_OK) {
        task_state = extract_json_value(chunk.data, "taskState");
    }
    
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    free(chunk.data);
    
    return task_state;
}

// Get task details
void sp_get_task_data(SPConfig *config, const char *session_id, const char *task_id) {
    CURL *curl;
    CURLcode res;
    Response chunk = {0};
    
    curl = curl_easy_init();
    if (!curl) return;
    
    chunk.data = malloc(1);
    chunk.size = 0;
    
    char url[MAX_URL_LEN];
    snprintf(url, sizeof(url), "%s/api/baclient/task/%s", 
             config->server_url, task_id);
    
    char session_header[512];
    snprintf(session_header, sizeof(session_header), "X-Session-Id: %s", session_id);
    
    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, "Accept: application/json");
    headers = curl_slist_append(headers, session_header);
    
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&chunk);
    
    res = curl_easy_perform(curl);
    
    if (res == CURLE_OK) {
        // Extract and display backup statistics
        char *total_files = extract_json_value(chunk.data, "totalFiles");
        char *completed_files = extract_json_value(chunk.data, "totalCompletedFiles");
        char *failed_files = extract_json_value(chunk.data, "totalFailedFiles");
        char *total_bytes = extract_json_value(chunk.data, "totalBytes");
        
        printf("\n%s\n", "======================================================================");
        printf("  BACKUP COMPLETED SUCCESSFULLY\n");
        printf("%s\n", "======================================================================");
        
        if (completed_files && total_files) {
            printf("Files processed: %s/%s\n", completed_files, total_files);
        }
        if (failed_files) {
            printf("Files failed: %s\n", failed_files);
        }
        if (total_bytes) {
            printf("Total size: %s bytes\n", total_bytes);
        }
        printf("Task ID: %s\n", task_id);
        
        free(total_files);
        free(completed_files);
        free(failed_files);
        free(total_bytes);
    }
    
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    free(chunk.data);
}

// Wait for task completion
int sp_wait_for_task(SPConfig *config, const char *session_id, const char *task_id, int max_minutes) {
    int max_attempts = max_minutes * 12; // Check every 5 seconds
    
    printf("\nWaiting for backup to complete (max %d minutes)...\n", max_minutes);
    
    for (int i = 0; i < max_attempts; i++) {
        char *task_state = sp_get_task_status(config, session_id, task_id);
        
        if (task_state) {
            if (strcmp(task_state, "Success") == 0) {
                printf("✓ Backup completed successfully\n");
                free(task_state);
                return 1;
            } else if (strcmp(task_state, "Failed") == 0 || strcmp(task_state, "Error") == 0) {
                fprintf(stderr, "✗ Backup failed with state: %s\n", task_state);
                free(task_state);
                return 0;
            } else if (strcmp(task_state, "Running") == 0 || strcmp(task_state, "Pending") == 0) {
                if (i % 6 == 0) { // Print every 30 seconds
                    printf("  Status: %s... (checking again in 5s)\n", task_state);
                }
            }
            free(task_state);
        }
        
        sleep(5);
    }
    
    fprintf(stderr, "✗ Timeout waiting for backup to complete\n");
    return 0;
}

// Sign off
void sp_sign_off(SPConfig *config, const char *session_id) {
    CURL *curl = curl_easy_init();
    if (!curl) return;
    
    char url[MAX_URL_LEN];
    snprintf(url, sizeof(url), "%s/api/baclient/signoff", config->server_url);
    
    char payload[512];
    snprintf(payload, sizeof(payload), "{\"sessionId\":\"%s\"}", session_id);
    
    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payload);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    
    printf("\nSigning off...\n");
    curl_easy_perform(curl);
    
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
}

// Scan directory for files
FileList* scan_directory(const char *dir_path) {
    FileList *list = malloc(sizeof(FileList));
    list->files = malloc(sizeof(char*) * MAX_FILES);
    list->count = 0;
    
    DIR *dir = opendir(dir_path);
    if (!dir) {
        fprintf(stderr, "Error: Cannot open directory '%s'\n", dir_path);
        return list;
    }
    
    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL && list->count < MAX_FILES) {
        if (entry->d_type == DT_REG) { // Regular file
            // Check for .txt extension (or any file)
            char *ext = strrchr(entry->d_name, '.');
            if (ext && strcmp(ext, ".txt") == 0) {
                char full_path[MAX_PATH_LEN];
                snprintf(full_path, sizeof(full_path), "%s/%s", dir_path, entry->d_name);
                
                list->files[list->count] = strdup(full_path);
                list->count++;
            }
        }
    }
    
    closedir(dir);
    return list;
}

// Free file list
void free_file_list(FileList *list) {
    if (!list) return;
    for (int i = 0; i < list->count; i++) {
        free(list->files[i]);
    }
    free(list->files);
    free(list);
}

int main(int argc, char *argv[]) {
    // Configuration
    SPConfig config = {
        .server_url = getenv("SP_SERVER_URL") ? getenv("SP_SERVER_URL") : "http://spserver:1580",
        .node_id = getenv("SP_NODE_ID") ? getenv("SP_NODE_ID") : "APPLEBEES",
        .password = getenv("SP_PASSWORD"),
        .backup_directory = getenv("SP_BACKUP_DIR") ? getenv("SP_BACKUP_DIR") : "/sp_backups/ceph_downloads"
    };
    
    const char *download_dir = argc > 1 ? argv[1] : "downloads";
    
    if (!config.password) {
        fprintf(stderr, "Error: SP_PASSWORD environment variable not set\n");
        fprintf(stderr, "\nUsage: %s [download_directory]\n", argv[0]);
        fprintf(stderr, "\nEnvironment variables:\n");
        fprintf(stderr, "  SP_SERVER_URL  - Storage Protect server (default: http://spserver:1580)\n");
        fprintf(stderr, "  SP_NODE_ID     - Node ID (default: APPLEBEES)\n");
        fprintf(stderr, "  SP_PASSWORD    - Password (required)\n");
        fprintf(stderr, "  SP_BACKUP_DIR  - Backup directory (default: /sp_backups/ceph_downloads)\n");
        return 1;
    }
    
    printf("%s\n", "======================================================================");
    printf("  IBM STORAGE PROTECT - C UPLOADER\n");
    printf("%s\n", "======================================================================");
    
    curl_global_init(CURL_GLOBAL_DEFAULT);
    
    // Scan for files
    printf("\nScanning directory: %s\n", download_dir);
    FileList *files = scan_directory(download_dir);
    
    if (files->count == 0) {
        fprintf(stderr, "WARNING: No .txt files found in %s\n", download_dir);
        free_file_list(files);
        curl_global_cleanup();
        return 0;
    }
    
    printf("Found %d file(s) to backup:\n", files->count);
    for (int i = 0; i < files->count; i++) {
        struct stat st;
        stat(files->files[i], &st);
        printf("  - %s (%ld bytes)\n", strrchr(files->files[i], '/') + 1, st.st_size);
    }
    
    // Sign on
    printf("\n%s\n", "======================================================================");
    printf("  SIGNING ON TO IBM STORAGE PROTECT\n");
    printf("%s\n", "======================================================================");
    
    char *session_id = NULL;
    char *signon_task_id = sp_sign_on(&config, &session_id);
    
    if (!session_id) {
        fprintf(stderr, "Failed to sign on\n");
        free_file_list(files);
        curl_global_cleanup();
        return 1;
    }
    
    // Create backup name with timestamp
    time_t now = time(NULL);
    struct tm *t = localtime(&now);
    char backup_name[128];
    strftime(backup_name, sizeof(backup_name), "ceph_downloads_%Y%m%d_%H%M%S", t);
    
    // Start backup
    char *backup_task_id = sp_start_backup(&config, session_id, download_dir, backup_name, files);
    
    if (!backup_task_id) {
        fprintf(stderr, "Failed to start backup\n");
        sp_sign_off(&config, session_id);
        free(session_id);
        free_file_list(files);
        curl_global_cleanup();
        return 1;
    }
    
    // Wait for completion
    int success = sp_wait_for_task(&config, session_id, backup_task_id, 10);
    
    if (success) {
        // Get detailed results
        sp_get_task_data(&config, session_id, backup_task_id);
    }
    
    // Cleanup
    sp_sign_off(&config, session_id);
    
    free(session_id);
    free(signon_task_id);
    free(backup_task_id);
    free_file_list(files);
    curl_global_cleanup();
    
    return success ? 0 : 1;
}