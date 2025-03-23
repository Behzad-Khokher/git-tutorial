package main

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"

	"flag"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/secretsmanager"
	"github.com/evalphobia/logrus_sentry"
	"github.com/go-playground/validator/v10"
	"github.com/gofrs/uuid"
	"github.com/golang-jwt/jwt"
	"github.com/gomodule/redigo/redis"
	"github.com/gorilla/sessions"
	"github.com/gorilla/websocket"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
	"github.com/urfave/negroni"
	"golang.org/x/crypto/ssh"
	"gopkg.in/boj/redistore.v1"

	"utils"
)

// postgreSQL database connection settings.

const (
	IntegrityConstraintViolationClass pq.ErrorClass = "23"
	IntegrityConstraintViolation      pq.ErrorCode  = "23000"
	RestrictViolation                 pq.ErrorCode  = "23001"
	NotNullViolation                  pq.ErrorCode  = "23502"
	ForeignKeyViolation               pq.ErrorCode  = "23503"
	UniqueViolation                   pq.ErrorCode  = "23505"
	CheckViolation                    pq.ErrorCode  = "23514"
	ExclusionViolation                pq.ErrorCode  = "23P01"
	UndefinedColumn                   pq.ErrorCode  = "42703"
)

// send email with SMTP settings.
const (
	SMTPHost     = "172.17.0.1"
	SMTPPort     = 7587
	SMTPUsername = "app1276"
	SMTPPassword = "1741901028717344228"
)

// JWT congig
const (
	accessTTL  = int64(45) // Access token expires in 45 minutes
	refreshTTL = int64(25) // Refresh token expires in 25 days
)

// flag values to indicate application runtime mode.
const (
	ProductionMode = "production"
	TestingMode    = "testing"
)

// Sentry (https://sentry.io/) 3rd party application monitoring service.
// Data Source Name (DSN) value to send event logs.
const sentryDSN = ""

const AppID = 1276

// Redis (https://redis.io/) open source in-memory data store settings.
// Set up to manage login sessions with HTTP headers.
// Set up to manage pagination data.
var (
	RediStoreMaxIdle = 1
	RediStoreNetwork = "tcp"

	RediStorePassword = ""

	RediStoreAuthenticationKey = os.Getenv("REDIS_PASSWORD")

	RediStoreEncryptionKey = ""

	LoginSessionName      = "session-key"
	SessionValueUserIDKey = "1276:user_id"
	InternalHeaderUserID  = "ih_user_id"

	RedisMaxActive    = 8
	RedisPaginationDB = 1

	PaginateTimeLimitSeconds    = 3600
	PaginateDataMaxWaitSeconds  = 10
	PaginateDataFieldNumTotal   = "num"
	PaginateDataFieldNumPerPage = "num_per_page"
	PaginateDataError           = "error"
	PaginateDataIsDB            = "is_db"

	// database specific pagination
	PaginationSchema        = "pagination"
	PaginationTableNameFmt  = "p-%s"
	PaginationTableRowIdCol = "row_id_123abc"

	// metric logger
	MetricSchema = "metric"
)

// directory to store files.
const FileSystemRoot = "1276"

// base URL to append attachments for access
const UploadsBaseURL = "dev.dittofi.link/1276/uploads"

// store runtime mode value.
var runtimeMode string

// store http server listening port.
var port string

// store http server host.
var host string

// store access to postgreSQL database.
var pg *sqlx.DB

// store access to log system.
var log *logrus.Logger

// store access to logged in sessions.
var loginSessions *redistore.RediStore

// store access to web and email templates.
var templates *template.Template

// store access to filesystem.
var fs *FileSystem

// store metric logger
var metricLogger utils.MetricLogger

// store access to paginated data.
var paginationDataStorage *redis.Pool

type CustomerAWSSecrets struct {
	RdsHost       string `json:"rds_host"`
	RdsDb         string `json:"rds_db"`
	RdsPort       string `json:"rds_port"`
	RdsUser       string `json:"rds_user"`
	RdsPassword   string `json:"rds_password"`
	RdsSchema     string `json:"rds_schema"`
	RedisUser     string `json:"redis_user"`
	RedisPassword string `json:"redis_password"`
	RedisUrl      string `json:"redis_url"`
	S3Bucket      string `json:"s3_bucket"`
}

type RefreshTokenRequest struct {
	RefreshToken string `json:"refresh_token"`
}

// function setting up and running http server.
// @title Dittofi API - My Super Powerful App (AppID: 1276)
// @version 1.0
// @description This is the endpoint documentation for the Dittofi App ID 1276.
// @termsOfService http://swagger.io/terms/
// @license.name MIT
// @license.url https://opensource.org/licenses/MIT
// @contact.name Dittofi Support Team
// @contact.email info@dittofi.com
// @contact.url https://www.dittofi.com/dittofi-support
// @BasePath /iapi
func main() {
	flag.StringVar(&port, "port", "8000", "the port the http server listens on")
	flag.StringVar(&host, "host", "0.0.0.0", "the host the http server listens on")
	flag.StringVar(&runtimeMode, "mode", TestingMode, "'testing' disables logging to sentry, 'production' enables logging to sentry")
	flag.Parse()

	log = setUpLogger()

	cm = NewConnectionManager()
	go func() {
		cronClient := cron.New(cron.WithParser(cron.NewParser(cron.SecondOptional|cron.Minute|cron.Hour|cron.Dom|cron.Month|cron.Dow|cron.Descriptor)), cron.WithLocation(time.UTC))
		cronClient.Run()
	}()

	var err error
	CustomerConfigSecretName := os.Getenv("CUSTOMER_CONFIG_SECRET_NAME")
	customerSecrets := CustomerAWSSecrets{
		RdsHost:       os.Getenv("DATABASE_HOST"),
		RdsDb:         os.Getenv("DATABASE_NAME"),
		RdsPort:       "5432",
		RdsUser:       os.Getenv("DATABASE_USER"),
		RdsPassword:   os.Getenv("RDS_PASSWORD"),
		RdsSchema:     os.Getenv("DATABASE_SCHEMA"),
		RedisUser:     os.Getenv("REDIS_USER"),
		RedisPassword: os.Getenv("REDIS_PASSWORD"),
		RedisUrl:      os.Getenv("REDIS_URL"),
		S3Bucket:      os.Getenv("S3_BUCKET"),
	}

	log.Info("CustomerConfigSecretName: ", CustomerConfigSecretName)
	log.Info("Customer Secrets Object: ", customerSecrets)

	if len(CustomerConfigSecretName) > 0 {
		var sess *session.Session
		sess, err = session.NewSession(&aws.Config{
			LogLevel:                      aws.LogLevel(aws.LogDebugWithHTTPBody),
			CredentialsChainVerboseErrors: aws.Bool(true),
			S3ForcePathStyle:              aws.Bool(true),
			Region:                        aws.String("us-west-2"),
		})
		if err != nil {
			log.Fatal(err)
		}

		secretsManager := secretsmanager.New(sess)

		var customerSecretValue *secretsmanager.GetSecretValueOutput
		customerSecretValue, err = secretsManager.GetSecretValue(&secretsmanager.GetSecretValueInput{
			SecretId: aws.String(CustomerConfigSecretName),
		})
		if err != nil {
			log.Fatal(err)
		}

		// --------------------------------------
		// Set environment variables from JSON
		// --------------------------------------
		if err = setSecretsAsEnv(*customerSecretValue.SecretString); err != nil {
			log.Fatal(err)
		}

		UpdateGlobalVariables()

		err = json.Unmarshal([]byte(*customerSecretValue.SecretString), &customerSecrets)
		if err != nil {
			log.Fatal(err)
		}
	}

	pg = connectPostgres(customerSecrets)
	loginSessions = configRediStore(customerSecrets)
	defer loginSessions.Close()
	fs = NewFileSystem(FileSystemRoot, customerSecrets)
	paginationDataStorage = configPaginationStorage(customerSecrets)

	// Create metric logger & start running in background
	metricLogger = utils.NewPostgreSQLMetricLogger(pg, "1276", MetricSchema, customerSecrets.RdsUser, 55)
	go utils.StartMetricLogger(metricLogger)

	router := configRouter()

	router.HandleFunc("/v1/refresh_token", RefreshTokenHandler).Methods("POST")

	n := negroni.New()
	n.Use(negroni.NewRecovery())
	n.Use(utils.NewCorsMiddleware("*"))
	n.Use(utils.NewMetricLoggerMiddleware(metricLogger))
	//n.Use(utils.NewRateLimiterMiddleware(16.67))
	n.UseHandler(router)
	n.Run(fmt.Sprintf("%s:%s", host, port))
}

func getEnvVar(name string) (string, error) {
	value, ok := os.LookupEnv(name)
	if !ok {
		return "", fmt.Errorf("environment variable %s not found", name)
	} else {
		return value, nil
	}
}

// setUpLoggers sets up access to the Sentry or system logger.
func setUpLogger() *logrus.Logger {
	log := logrus.New()

	// log to Sentry
	if runtimeMode == ProductionMode {
		hook, err := logrus_sentry.NewSentryHook(sentryDSN, []logrus.Level{
			logrus.PanicLevel,
			logrus.FatalLevel,
			logrus.ErrorLevel,
		})

		if err != nil {
			log.Fatal(err)
		} else {
			hook.StacktraceConfiguration.Enable = true
			hook.StacktraceConfiguration.Skip = 0
			hook.StacktraceConfiguration.Context = 2
			log.Hooks.Add(hook)
		}
	}

	return log
}

// connectPostgres sets up access to postgreSQL database.
func connectPostgres(secrets CustomerAWSSecrets) *sqlx.DB {
	if pg != nil {
		// already connected
		return pg
	}

	log.WithFields(logrus.Fields{
		"db_host": secrets.RdsHost,
	}).Info("Connect to PostgreSQL: ...")

	var searchPath string
	if len(secrets.RdsSchema) > 0 {
		searchPath = fmt.Sprintf(" search_path=%s", secrets.RdsSchema)
	}

	connString := fmt.Sprintf("dbname=%s user=%s password=%s host=%s sslmode=disable%s",
		secrets.RdsDb,
		secrets.RdsUser,
		secrets.RdsPassword,
		secrets.RdsHost,
		searchPath,
	)

	log.Info("Connection String: ", connString)

	pg = sqlx.MustConnect("postgres", connString)
	//pg.Exec(fmt.Sprintf("set search_path='%s'", "app_1276"))
	pg.SetMaxIdleConns(1)
	pg.SetMaxOpenConns(32)
	log.Info("... Connected to PostgreSQL")

	return pg
}

// configRediStore sets up access to Redis for login sessions.
func configRediStore(secrets CustomerAWSSecrets) *redistore.RediStore {
	var keyPairs [][]byte
	if len(secrets.RedisPassword) < 1 {
		log.Fatal("no authentication key set for RediStore")
	} else {
		keyPairs = append(keyPairs, []byte(secrets.RedisPassword))
	}

	if l := len(RediStoreEncryptionKey); l > 0 {
		if l == 16 || l == 24 || l == 32 {
			keyPairs = append(keyPairs, []byte(RediStoreEncryptionKey))
		} else {
			log.Fatal("wrong length for encryption key set for RediStore")
		}
	}

	store, err := redistore.NewRediStore(RediStoreMaxIdle, RediStoreNetwork, secrets.RedisUrl, RediStorePassword, keyPairs...)
	if err != nil {
		log.Fatal(err)
	}

	store.Options.Path = "/"
	store.Options.SameSite = http.SameSiteNoneMode
	store.Options.Secure = true

	return store
}

// addLoginSession stores session and user id of logged in user into Redis.
func addLoginSession(w *http.ResponseWriter, r *http.Request, userID int) (err error) {
	// Get a session.
	var loginSession *sessions.Session
	loginSession, err = loginSessions.Get(r, LoginSessionName)
	if err != nil {
		return
	}

	// Add a value.
	loginSession.Values[SessionValueUserIDKey] = userID

	// Save.
	err = sessions.Save(r, *w)

	return
}

// removeLoginSession removes stored session and user id from Redis.
func removeLoginSession(w *http.ResponseWriter, r *http.Request) (err error) {
	// Get the session.
	var loginSession *sessions.Session
	loginSession, err = loginSessions.Get(r, LoginSessionName)
	if err != nil {
		return
	}

	// Delete session.
	loginSession.Options.MaxAge = -1

	// Save.
	err = sessions.Save(r, *w)

	return
}

// getLoginSessionUserID retrieves the user id of logged in user session from Redis.
// Attempts to look for internal HTTP request header containing user id already set.
func getLoginSessionUserID(r *http.Request) (userID int, err error) {
	// attempt to see if internal header set already otherwise attempt to find
	var userIDStr = r.Header.Get(InternalHeaderUserID)
	if len(userIDStr) == 0 {
		// Get a session.

		var loginSession *sessions.Session
		loginSession, err = loginSessions.Get(r, LoginSessionName)
		if err != nil {
			return
		}

		if iUserID, ok := loginSession.Values[SessionValueUserIDKey]; !ok {
			err = fmt.Errorf("no user id found")
			return
		} else if userID, ok = iUserID.(int); !ok {
			err = fmt.Errorf("unexpected user id type not int")
			return
		}
	} else {
		userID, err = strconv.Atoi(userIDStr)
	}

	return
}

func getUserIDFromToken(r *http.Request) (int, error) {
	payload, err := ValidateJwtToken(r, RediStoreAuthenticationKey)
	if err != nil {
		return 0, err
	}

	// Attempt to assert the payload to an int
	userID, ok := payload.(float64) // JWT often encodes numbers as float64
	if !ok {
		return 0, fmt.Errorf("failed to parse user ID from token payload")
	}

	return int(userID), nil
}

// RequireLogin wraps handler to check login before processing request.
func RequireLogin(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, err := getLoginSessionUserID(r)
		if err != nil {
			w.Header().Set("X-Auth-Flow", "Session")
			utils.JSON(w, http.StatusUnauthorized, utils.Response{Error: &utils.ValError{Code: utils.ErrCodeUnAuthorized, Message: err.Error(), Param: "password"}})
			return
		}

		r.Header.Set(InternalHeaderUserID, fmt.Sprint(userID))
		handler(w, r)
	}
}

func RequireLoginJwt(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, err := ValidateJwtToken(r, RediStoreAuthenticationKey)
		if err != nil {
			w.Header().Set("X-Auth-Flow", "JWT")
			utils.JSON(w, http.StatusUnauthorized, utils.Response{Error: &utils.ValError{Code: utils.ErrCodeUnAuthorized, Message: err.Error(), Param: "password"}})
			return
		}

		r.Header.Set(InternalHeaderUserID, fmt.Sprint(userID))
		handler(w, r)
	}
}

// manage concurrent access to files.
type FileSystem struct {
	// control concurrent access to FileLocks
	sync.Mutex

	// control concurrent read/write access to files
	FileLocks map[string]*sync.RWMutex
	Root      string

	TempDir string

	// Store files in S3
	S3Client *utils.S3Client
}

// NewFileSystem sets up access to S3 file system.
func NewFileSystem(root string, secrets CustomerAWSSecrets) (fs *FileSystem) {
	if len(root) == 0 || root == "/" {
		log.Fatalf(`invalid file system root "%s" set`, root)
	}

	fs = &FileSystem{
		Mutex:     sync.Mutex{},
		FileLocks: make(map[string]*sync.RWMutex, 0),
		Root:      root,
		TempDir:   "temp",
		S3Client:  s3Client,
	}

	return
}

// GetFileLock obtain lock to access file.
func (fs *FileSystem) GetFileLock(path string) (lock *sync.RWMutex) {
	fs.Lock()
	defer fs.Unlock()
	lock, ok := fs.FileLocks[path]
	if !ok {
		lock = &sync.RWMutex{}
		fs.FileLocks[path] = lock
	}

	return
}

// Open returns access to a file.
func (fs *FileSystem) Open(name string) (f File, err error) {
	lock := fs.GetFileLock(name)
	lock.RLock()
	defer lock.RUnlock()

	// generate full path for final store location
	var fullPath = name
	if len(fs.Root) > 0 {
		fullPath = filepath.Join(fs.Root, name)
	}

	// Open file.
	var b []byte
	b, _, err = fs.S3Client.GetFileFromBucket(fullPath)
	if err != nil {
		return
	}

	f = File{ReadSeeker: bytes.NewReader(b)}

	return
}

// SetFile save the file in S3.
func (fs *FileSystem) SetFile(f File, path string, overwrite bool) (url string, err error) {
	lock := fs.GetFileLock(path)
	lock.Lock()
	defer lock.Unlock()

	// generate full path for final store location
	var fullPath = path
	if len(fs.Root) > 0 {
		fullPath = filepath.Join(fs.Root, path)
	}

	if overwrite != true {
		// error if file exists
	}

	var contentType string
	contentType, err = GetFileContentType(f)
	if err != nil {
		return
	}

	url, err = fs.S3Client.UploadFile(f, fullPath, contentType)
	if err != nil {
		return
	}

	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		return
	}

	return
}

// SetTempFile temporary stores data into a file.
func (fs *FileSystem) SetTempFile(data io.Reader) (f File, err error) {
	var b []byte
	b, err = ioutil.ReadAll(data)
	if err != nil {
		return
	}

	f = File{ReadSeeker: bytes.NewReader(b)}
	return
}

// GetFileContentType returns the content type of a file.
func GetFileContentType(f File) (contentType string, err error) {
	// to sniff the content type only the first 512 bytes are used.
	buf := make([]byte, 512)

	_, err = f.Read(buf)
	if err != nil {
		return
	}

	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		return
	}

	contentType = http.DetectContentType(buf)

	return
}

// PublicKey generates authentication method from private key for SSH.
func PublicKey(privateKey string) (*ssh.AuthMethod, error) {
	signer, err := ssh.ParsePrivateKey([]byte(privateKey))
	if err != nil {
		return nil, err
	}

	authMethod := ssh.PublicKeys(signer)
	return &authMethod, nil
}

// Generates JWT Token for Authentication
func GenerateJwtToken(signingMethod, privateKey string, payload interface{}, ttl int64) (token string, err error) {
	var key interface{}
	if signingMethod == "jwt.SigningMethodRS256" {
		key, err = jwt.ParseRSAPrivateKeyFromPEM([]byte(privateKey))
		if err != nil {
			return "", fmt.Errorf("create: parse key: %w", err)
		}
	} else if signingMethod == "jwt.SigningMethodHS256" {
		key = []byte(privateKey)
	} else {
		return "", fmt.Errorf("create: invalid signing method: %w", err)
	}

	now := time.Now().UTC()

	claims := make(jwt.MapClaims)
	claims["payload"] = payload                                    // Our custom data.
	claims["exp"] = now.Add(time.Duration(ttl) * time.Hour).Unix() // The expiration time after which the token must be disregarded.
	claims["iat"] = now.Unix()                                     // The time at which the token was issued.
	claims["nbf"] = now.Unix()                                     // The time before which the token must be disregarded.

	if signingMethod == "jwt.SigningMethodRS256" {
		token, err = jwt.NewWithClaims(jwt.SigningMethodRS256, claims).SignedString(key)
	} else if signingMethod == "jwt.SigningMethodHS256" {
		token, err = jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(key)
	} else {
		return "", fmt.Errorf("create: invalid signing method: %w", err)
	}

	// To avoid key declared but not used error incase not used in if statements
	_ = key

	if err != nil {
		return "", fmt.Errorf("create: sign token: %w", err)
	}
	return
}

// Generates JWT Token for Authentication
func GenerateJwtAuthToken(privateKey string, payload interface{}, accessTTL, refreshTTL int64) (token string, refreshToken string, err error) {
	key := []byte(privateKey)
	now := time.Now().UTC()

	claims := make(jwt.MapClaims)
	claims["payload"] = payload                                            // Our custom data.
	claims["exp"] = now.Add(time.Duration(accessTTL) * time.Minute).Unix() // The expiration time after which the token must be disregarded.
	claims["iat"] = now.Unix()                                             // The time at which the token was issued.
	claims["nbf"] = now.Unix()                                             // The time before which the token must be disregarded.

	// Generate the access token
	token, err = jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(key)
	if err != nil {
		return "", "", fmt.Errorf("create: sign access token: %w", err)
	}

	// Generate refresh token
	refreshClaims := jwt.MapClaims{
		"payload": payload,
		"exp":     now.Add(time.Duration(refreshTTL) * 24 * time.Hour).Unix(), // Refresh token expiry 7
		"iat":     now.Unix(),
	}
	refreshToken, err = jwt.NewWithClaims(jwt.SigningMethodHS256, refreshClaims).SignedString(key)
	if err != nil {
		return "", "", fmt.Errorf("create: sign refresh token: %w", err)
	}

	return token, refreshToken, nil
}

// ValidateJwtToken validates a JWT token and extracts the payload.
func ValidateJwtToken(r *http.Request, privateKey string) (interface{}, error) {
	key := []byte(privateKey)

	// Extract the token from the Authorization header
	authHeader := r.Header.Get("Authorization")
	tokenString := strings.TrimPrefix(authHeader, "Bearer ")

	if tokenString == "" {
		return nil, fmt.Errorf("authorization header is missing")
	}

	// Parse the JWT token
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		// Ensure the signing method is HS256
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return key, nil
	})

	// Validate the token and check for errors
	if err != nil || !token.Valid {
		return nil, fmt.Errorf("invalid token: %w", err)
	}

	// Extract claims and return the payload
	if claims, ok := token.Claims.(jwt.MapClaims); ok && token.Valid {
		payload, ok := claims["payload"]
		if !ok {
			return nil, fmt.Errorf("missing payload in token")
		}
		return payload, nil
	}

	return nil, fmt.Errorf("invalid token claims")
}

// RefreshTokenHandler handles the refresh token requests
// @Summary Refreshes the access token using a valid refresh token
// @Description This endpoint accepts a refresh token and returns a new access token and refresh token.
// @Tags Authentication
// @Accept json
// @Produce json
// @Param refresh_token body RefreshTokenRequest true "Refresh Token"
// @Success 200 {object} RefreshTokenResponse "returns a new access and refresh token"
// @Failure 400 {string} string "invalid request body"
// @Failure 400 {string} string "refresh token is required"
// @Failure 401 {string} string "invalid refresh token"
// @Failure 500 {string} string "failed to generate tokens"
// @Security BearerAuth
// @Router /v1/refresh_token [post]
func RefreshTokenHandler(w http.ResponseWriter, r *http.Request) {
	// Parse the request body
	var req RefreshTokenRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.RefreshToken == "" {
		http.Error(w, "refresh token is required", http.StatusBadRequest)
		return
	}

	// Validate the refresh token
	payload, err := ValidateRefreshToken(req.RefreshToken, RediStoreAuthenticationKey)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid refresh token: %v", err), http.StatusUnauthorized)
		return
	}

	privateKey := RediStoreAuthenticationKey
	// Generate new tokens
	newAccessToken, newRefreshToken, err := GenerateJwtAuthToken(privateKey, payload, accessTTL, refreshTTL)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to generate tokens: %v", err), http.StatusInternalServerError)
		return
	}

	// Send the new tokens in the response
	response := map[string]string{
		"access_token":  newAccessToken,
		"refresh_token": newRefreshToken,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func ValidateRefreshToken(refreshToken, privateKey string) (interface{}, error) {
	key := []byte(privateKey)

	// Parse the refresh token
	token, err := jwt.Parse(refreshToken, func(token *jwt.Token) (interface{}, error) {
		// Ensure the signing method is HS256
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return key, nil
	})

	// Validate the token and check for errors
	if err != nil || !token.Valid {
		return nil, fmt.Errorf("invalid token: %w", err)
	}

	// Extract claims and return the payload
	if claims, ok := token.Claims.(jwt.MapClaims); ok && token.Valid {
		payload, ok := claims["payload"]
		if !ok {
			return nil, fmt.Errorf("missing payload in token")
		}
		return payload, nil
	}

	return nil, fmt.Errorf("invalid token claims")
}

// DBPaginationRemover manages removing database paginated data.
func DBPaginationRemover(redisPool *redis.Pool) {
	var removePaginationTables = func() {
		// recover from panics
		defer func() {
			if r := recover(); r != nil {
				log.Println("recover", r)
				debug.PrintStack()
			}
		}()

		var IDs []string
		err := pg.Select(&IDs, fmt.Sprintf(`
DELETE FROM %s.pagination_logs WHERE now() - ts > ($1 || ' seconds')::INTERVAL RETURNING id
`, PaginationSchema), PaginateDataMaxWaitSeconds)
		if err != nil {
			log.WithError(err).Errorf("Failed to remove outdated pagination ids from pagination_logs.")
			return
		}

		conn := redisPool.Get()
		defer conn.Close()

		for _, id := range IDs {
			// delete record from redis
			_, err = conn.Do("DEL", id)
			if err != nil {
				log.WithError(err).Errorf("Failed to remove pagination from redis.")
			}

			// delete table from database
			tableName := fmt.Sprintf(PaginationTableNameFmt, id)
			_, err = pg.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s."%s"`, PaginationSchema, tableName))
			if err != nil {
				log.WithError(err).Errorf("Failed to remove outdated pagination table.")
				continue
			}
		}
	}

	// clean every half PaginateTimeLimitSeconds time passed
	ticker := time.NewTicker(time.Duration(PaginateTimeLimitSeconds/2) * time.Second)
	for {
		select {
		case <-ticker.C:
			go removePaginationTables()
		}
	}
}

// configPaginationStorage sets up access to Redis/database for paginated data.
func configPaginationStorage(secrets CustomerAWSSecrets) *redis.Pool {
	var err error

	// create redis pool
	redisPool := redis.Pool{
		MaxIdle:     RediStoreMaxIdle,
		MaxActive:   RedisMaxActive,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			options := []redis.DialOption{
				redis.DialDatabase(1),
			}

			if len(RediStorePassword) > 0 {
				options = append(options, redis.DialPassword(RediStorePassword))
			}

			return redis.Dial(RediStoreNetwork, secrets.RedisUrl, options...)
		},
		Wait: true,
	}

	// create pagination schema
	_, err = pg.Exec(fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s AUTHORIZATION %s`, PaginationSchema, secrets.RdsUser))
	if err != nil {
		log.Fatal(err)
	}

	// create pagination log table to store database pagination ids
	_, err = pg.Exec(fmt.Sprintf(`
                CREATE TABLE IF NOT EXISTS %s.pagination_logs (
                        id VARCHAR,
                        ts TIMESTAMPTZ NOT NULL DEFAULT now(),
                        PRIMARY KEY (id)
                )
        `, PaginationSchema))
	if err != nil {
		log.Fatal(err)
	}

	// start db pagination remover
	go DBPaginationRemover(&redisPool)

	return &redisPool
}

// store data for page of paginated data.
type PaginatedPageData struct {
	Data json.RawMessage `json:"data"`
	// number of elements in Data
	NumElements int `json:"num_elements"`
	// flag to indicate if last page of Data
	IsLast bool `json:"is_last"`
}

// output for paginated data.
type PaginatedData struct {
	PaginatedPageData
	// total number of all elements
	NumTotal int `json:"num_total"`
	// relative to PaginatedData.NumElements, 0 indexed
	ElementIndex int `json:"element_index"`
}

// EncodePageDataFn function signature to build PaginatedPageData
// i is index for start of slice of elements (inclusive)
// j is index for end of slice of elements (exclusive) must handle out of range
type EncodePageDataFn func(i, j int) (PaginatedPageData, error)

// PaginateData stores key with fields for numPerPage, numTotal and pages in redis.
func PaginateData(key string, numPerPage, numTotal int, getEncodedData EncodePageDataFn) (err error) {
	key = fmt.Sprintf(`%d:%s`, AppID, key)

	const (
		setNotExistCmd = "HSETNX"
		setCmd         = "HSET"
		expireCmd      = "EXPIRE"
	)

	// get connection to redis
	conn := paginationDataStorage.Get()
	defer conn.Close()

	// set pagination metadata total number of elements
	n, err := redis.Int(conn.Do(setNotExistCmd, key, PaginateDataFieldNumTotal, numTotal))
	if err != nil {
		return
	} else if n == 0 {
		err = fmt.Errorf("key (%s) already exists", key)
		return
	}
	// remove key upon returning with error
	defer func(e *error) {
		if *e != nil {
			conn := paginationDataStorage.Get()
			defer conn.Close()

			if _, err := conn.Do("DEL", key); err != nil {
				log.WithError(err).Errorf("Failed to remove key from redis.")
				return
			}
		}
	}(&err)

	// set pagination metadata number of elements per page and is_db flag
	n, err = redis.Int(conn.Do(setCmd, key, PaginateDataFieldNumPerPage, numPerPage, PaginateDataIsDB, false))
	if err != nil {
		return
	} else if n < 2 {
		err = fmt.Errorf("error setting key (%s) num_per_page/is_db field", key)
		return
	}

	// set the first paginated page
	page, err := getEncodedData(0, numPerPage)
	if err != nil {
		return
	}
	value, err := json.Marshal(page)
	if err != nil {
		return
	}

	pageNum := 0
	n, err = redis.Int(conn.Do(setCmd, key, pageNum, value))
	if err != nil {
		return
	} else if n == 0 {
		err = fmt.Errorf("error setting key (%s) page (%d)", key, pageNum)
		return
	}

	// set key expiration
	n, err = redis.Int(conn.Do(expireCmd, key, PaginateTimeLimitSeconds))
	if err != nil {
		return
	} else if n == 0 {
		err = fmt.Errorf("error setting key (%s) expiry", key)
		return
	}

	// set remaining pages in goroutine
	go func() {
		setErrorFn := func(conn redis.Conn, err error) {
			log.Errorf(err.Error())

			n, err = redis.Int(conn.Do(setCmd, key, PaginateDataError, err.Error()))
			if err != nil {
				log.WithError(err).Errorf("error setting key (%s) error field", key)
			} else if n == 0 {
				log.Errorf("failed to set key (%s) error field", key)
			}
		}

		// recover from panics
		defer func() {
			if r := recover(); r != nil {
				log.Println("recover", r)
				debug.PrintStack()

				// get connection to redis
				conn := paginationDataStorage.Get()
				defer conn.Close()

				setErrorFn(conn, fmt.Errorf("encountered a panic saving pagination data pages"))
			}
		}()

		// get connection to redis
		conn := paginationDataStorage.Get()
		defer conn.Close()

		for i := numPerPage; i < numTotal; i += numPerPage {
			page, err := getEncodedData(i, i+numPerPage)
			if err != nil {
				setErrorFn(conn, err)
				return
			}

			value, err := json.Marshal(page)
			if err != nil {
				setErrorFn(conn, err)
				return
			}

			pageNum++
			n, err = redis.Int(conn.Do(setCmd, key, pageNum, value))
			if err != nil {
				setErrorFn(conn, err)
				return
			} else if n == 0 {
				setErrorFn(conn, fmt.Errorf("error setting key (%s) page (%d)", key, pageNum))
				return
			}
		}
	}()

	return nil
}

// PaginateDBData stores key with fields for numPerPage, numTotal in redis and data in database.
func PaginateDBData(numPerPage int, statement string, args ...interface{}) (key string, err error) {
	const (
		setNotExistCmd = "HSETNX"
		setCmd         = "HSET"
		expireCmd      = "EXPIRE"
	)

	// generate key
	key, err = GeneratePaginationKey()
	if err != nil {
		return
	}

	// get connection to redis
	conn := paginationDataStorage.Get()
	defer conn.Close()

	// set pagination metadata number of elements per page
	n, err := redis.Int(conn.Do(setNotExistCmd, key, PaginateDataFieldNumPerPage, numPerPage))
	if err != nil {
		return
	} else if n == 0 {
		err = fmt.Errorf("key (%s) already exists", key)
		return
	}
	// remove key upon returning with error
	defer func(e *error) {
		if *e != nil {
			conn := paginationDataStorage.Get()
			defer conn.Close()

			if _, err := conn.Do("DEL", key); err != nil {
				log.WithError(err).Errorf("Failed to remove key from redis.")
				return
			}
		}
	}(&err)

	// set key expiration
	n, err = redis.Int(conn.Do(expireCmd, key, PaginateTimeLimitSeconds))
	if err != nil {
		return
	} else if n == 0 {
		err = fmt.Errorf("error setting key (%s) expiry", key)
		return
	}

	tx, err := pg.Beginx()
	if err != nil {
		return
	}
	defer tx.Rollback()

	_, err = tx.Exec(fmt.Sprintf(`INSERT INTO %s.pagination_logs(id) VALUES ($1)`, PaginationSchema), key)
	if err != nil {
		return
	}

	// create new pagination table to store the query output and add row number column for indexing
	tableName := fmt.Sprintf(PaginationTableNameFmt, key)
	_, err = tx.Exec(fmt.Sprintf(`
CREATE UNLOGGED TABLE %s."%s"
WITH (fillfactor=100)
AS
SELECT *, row_number() OVER () AS %s
FROM (%s) AS data_table
WITH DATA
`, PaginationSchema, tableName, PaginationTableRowIdCol, statement), args...)
	if err != nil {
		return
	}

	// make PaginationTableRowIdCol primary key for pagination table
	_, err = tx.Exec(fmt.Sprintf(`
ALTER TABLE %s."%s" ADD PRIMARY KEY (%s)
`, PaginationSchema, tableName, PaginationTableRowIdCol))
	if err != nil {
		return
	}

	var numTotal int
	err = tx.Get(&numTotal, fmt.Sprintf(`SELECT count(*) FROM %s."%s"`, PaginationSchema, tableName))
	if err != nil {
		return
	}

	// set pagination metadata total number of elements and is_db flag
	n, err = redis.Int(conn.Do(setCmd, key, PaginateDataFieldNumTotal, numTotal, PaginateDataIsDB, true))
	if err != nil {
		return
	} else if n < 2 {
		err = fmt.Errorf("error setting key (%s) num_per_page/is_db field", key)
		return
	}

	err = tx.Commit()
	if err != nil {
		return
	}

	return
}

// GetPaginatedMetadata retrieves metadata for pagination key.
func GetPaginatedMetadata(connPtr *redis.Conn, key string) (numTotal, numPerPage int, isDB bool, err error) {
	const (
		getMultiCmd = "HMGET"
		getCmd      = "HGET"
	)

	var conn redis.Conn
	if connPtr == nil {
		conn = paginationDataStorage.Get()
		defer conn.Close()
	} else {
		conn = *connPtr
	}

	// get pagination metadata number total and number per page
	is, err := redis.Ints(conn.Do(getMultiCmd, key, PaginateDataFieldNumTotal, PaginateDataFieldNumPerPage))
	if err != nil {
		return
	} else if len(is) < 2 {
		err = fmt.Errorf("invalid number (%d) of fields returned expected at least 2", len(is))
		return
	} else {
		numTotal = is[0]
		numPerPage = is[1]
	}

	// get is db flag
	isDB, err = redis.Bool(conn.Do(getCmd, key, PaginateDataIsDB))
	if err != nil {
		return
	}

	return
}

// GetPaginatedData retrieves a page of data for key containing elementIndex starting from 0.
func GetPaginatedData(key string, elementIndex int) (PaginatedData, error) {
	const getCmd = "HGET"

	// get connection to redis
	conn := paginationDataStorage.Get()
	defer conn.Close()

	// get pagination metadata
	var paginatedData PaginatedData
	numTotal, numPerPage, isDB, err := GetPaginatedMetadata(&conn, key)
	if err != nil {
		return PaginatedData{}, err
	} else if isDB {
		return PaginatedData{}, fmt.Errorf("unsupported data retrieval for data from database")
	} else {
		paginatedData.NumTotal = numTotal
	}

	// get paginated data
	if numPerPage > 0 && paginatedData.NumTotal > 0 {
		// check elementIndex is valid
		if elementIndex >= paginatedData.NumTotal {
			return PaginatedData{}, fmt.Errorf("element index (%d) out of range", elementIndex)
		}

		page := elementIndex / numPerPage

		// attempt to get data since PaginateData goroutine may not be done saving
		var b []byte
		for start := time.Now(); time.Now().Sub(start) < time.Duration(PaginateDataMaxWaitSeconds)*time.Second; time.Sleep(time.Second) {
			// check for errors
			var errStr string
			errStr, err = redis.String(conn.Do(getCmd, key, PaginateDataError))
			if err != nil && !errors.Is(err, redis.ErrNil) {
				break
			} else if len(errStr) > 0 {
				err = fmt.Errorf(errStr)
				break
			}

			b, err = redis.Bytes(conn.Do(getCmd, key, page))
			if err == nil {
				break
			}
		}
		if err != nil {
			return PaginatedData{}, err
		}

		err = json.Unmarshal(b, &paginatedData.PaginatedPageData)
		if err != nil {
			return PaginatedData{}, err
		}

		paginatedData.ElementIndex = elementIndex % numPerPage
	} else {
		b, err := redis.Bytes(conn.Do(getCmd, key, 0))
		if err != nil {
			return PaginatedData{}, err
		}

		err = json.Unmarshal(b, &paginatedData.PaginatedPageData)
		if err != nil {
			return PaginatedData{}, err
		}
	}

	return paginatedData, nil
}

// GeneratePaginationKey function to generate a key for pagination data.
func GeneratePaginationKey() (key string, err error) {
	var uuidKey uuid.UUID
	uuidKey, err = uuid.NewV4()
	if err != nil {
		return
	}

	return uuidKey.String(), nil
}

func DecodeJwtToken(tokenString, key, signingMethod string, makePayload func(data []byte) error) (err error) {
	// parse token
	var decodedToken *jwt.Token
	decodedToken, err = jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if token.Method.Alg() != signingMethod {
			return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
		}

		if token.Method.Alg() == "RS256" {
			key, err := jwt.ParseRSAPublicKeyFromPEM([]byte(key))
			if err != nil {
				return "", fmt.Errorf("validate: parse key: %w", err)
			}
			return key, nil
		} else if token.Method.Alg() == "HS256" {
			return []byte(key), nil
		} else {
			return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
		}
		// validate token
		return []byte(key), nil
	})

	if err != nil {
		err = fmt.Errorf("validate: parse token: %w", err)
		return
	}

	// validate claims
	claims, ok := decodedToken.Claims.(jwt.MapClaims)
	if !ok || !decodedToken.Valid {
		err = fmt.Errorf("Invalid token")
		return
	}

	// unmarshal claims into payload
	var ba []byte
	ba, err = json.Marshal(claims)
	if err != nil {
		err = fmt.Errorf("validate: marshal claims: %w", err)
		return
	}

	err = makePayload(ba)
	if err != nil {
		err = fmt.Errorf("validate: unmarshal claims: %w", err)
		return
	}

	return
}

// GeneratePassword to generate a password
func GeneratePassword(passwordLength int, symbol, lower, upper, number bool) (string, error) {
	lowerCharSet := "abcdedfghijklmnopqrst"
	upperCharSet := "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	symbolCharSet := "!@#$%&*"
	numberSet := "0123456789"
	var allCharSet string
	var password strings.Builder

	if lower {
		allCharSet += lowerCharSet
	}
	if upper {
		allCharSet += upperCharSet
	}
	if symbol {
		allCharSet += symbolCharSet
	}
	if number {
		allCharSet += numberSet
	}

	for i := 0; i < passwordLength; i++ {
		nBig, err := rand.Int(rand.Reader, big.NewInt(int64(len(allCharSet))))
		if err != nil {
			return "", err
		}

		random := int(nBig.Int64())
		password.WriteString(string(allCharSet[random]))
	}

	return password.String(), nil
}

// Convert a validator error to a string
func MsgToTag(fe validator.FieldError) string {
	switch fe.Tag() {
	case "required":
		return "This field is required"
	case "email":
		return "Invalid email"
	case "gte":
		return "Value must be greater than or equal to " + fe.Param()
	case "lte":
		return "Value must be less than or equal to " + fe.Param()
	case "min":
		return "Value must be greater than " + fe.Param()
	case "max":
		return "Value must be less than " + fe.Param()
	case "eq":
		return "Value must be equal to " + fe.Param()
	case "ne":
		return "Value must not be equal to " + fe.Param()
	case "oneof":
		return "Value must be one of " + fe.Param()
	case "contains":
		return "Value must contain " + fe.Param()
	case "url":
		return "Invalid URL"
	case "uuid":
		return "Invalid UUID"
	}
	return fe.Error() // default error
}

func FormatErrorResponse(err error) (response utils.Response, code int) {
	// Set default errors
	code = http.StatusInternalServerError
	valError := &utils.ValError{
		Code:    utils.ErrCodeInternalError,
		Message: err.Error(),
	}

	if pqErr, ok := err.(*pq.Error); ok {
		switch pqErr.Code {
		case UniqueViolation:
			valError = &utils.ValError{
				Code:    utils.ErrCodeUniqueViolation,
				Param:   pqErr.Constraint,
				Message: err.Error(),
			}
			code = http.StatusConflict
		case ForeignKeyViolation:
			valError = &utils.ValError{
				Code:    utils.ErrCodeForeignKeyViolation,
				Param:   pqErr.Constraint,
				Message: err.Error(),
			}
			code = http.StatusConflict
		case CheckViolation:
			valError = &utils.ValError{
				Code:    utils.ErrCodeCheckViolation,
				Param:   pqErr.Constraint,
				Message: err.Error(),
			}
			code = http.StatusConflict
		case ExclusionViolation:
			valError = &utils.ValError{
				Code:    utils.ErrCodeExclusionViolation,
				Param:   pqErr.Constraint,
				Message: err.Error(),
			}
			code = http.StatusConflict
		case NotNullViolation:
			valError = &utils.ValError{
				Code:    utils.ErrCodeNotNullViolation,
				Param:   pqErr.Constraint,
				Message: err.Error(),
			}
			code = http.StatusConflict
		case RestrictViolation:
			valError = &utils.ValError{
				Code:    utils.ErrCodeRestrictViolation,
				Param:   pqErr.Constraint,
				Message: err.Error(),
			}
			code = http.StatusConflict
		case IntegrityConstraintViolation:
			valError = &utils.ValError{
				Code:    utils.ErrCodeIntegrityConstraintViolation,
				Param:   pqErr.Constraint,
				Message: err.Error(),
			}
			code = http.StatusConflict
		case UndefinedColumn:
			valError = &utils.ValError{
				Code:    utils.ErrCodeUndefinedColumn,
				Param:   pqErr.Constraint,
				Message: err.Error(),
			}
			code = http.StatusInternalServerError
		default:
			valError = &utils.ValError{
				Code:    utils.ErrCodeInternalError,
				Message: err.Error(),
			}
			code = http.StatusInternalServerError
		}
	}

	response = utils.Response{Error: valError}
	return
}

// Handle websocket connections.
var cm *ConnectionManager

var upgrader = websocket.Upgrader{
	ReadBufferSize:  0,
	WriteBufferSize: 0,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

type ConnectionManager struct {
	connections map[interface{}]*websocket.Conn
	addCh       chan *AddConnectionRequest
	removeCh    chan interface{}
	sendCh      chan Message
}

type Message struct {
	ConnectionID interface{}
	Data         []byte
}

type AddConnectionRequest struct {
	Conn *websocket.Conn
	Key  interface{}
}

type WSPacket struct {
	Type      string      `json:"type"`
	Message   interface{} `json:"message"`
	Error     *string     `json:"error"`
	TSSending time.Time   `json:"ts_sent"`
}

func NewConnectionManager() *ConnectionManager {
	if cm != nil {
		return cm
	}

	cm = &ConnectionManager{
		connections: make(map[interface{}]*websocket.Conn),
		addCh:       make(chan *AddConnectionRequest),
		removeCh:    make(chan interface{}),
		sendCh:      make(chan Message),
	}

	go cm.start()

	return cm
}

func (cm *ConnectionManager) start() {
	for {
		select {
		case request := <-cm.addCh:
			cm.connections[request.Key] = request.Conn
			log.Info("Added websocket connection")
		case key := <-cm.removeCh:
			delete(cm.connections, key)
		case msg := <-cm.sendCh:
			conn := cm.connections[msg.ConnectionID]
			if conn != nil {
				err := conn.WriteMessage(websocket.TextMessage, msg.Data)
				if err != nil {
					log.Println("Error sending message to WebSocket connection:", err)
				}
			} else {
				log.Warn("Could not find websocket connection ")
			}
		}
	}
}

func (cm *ConnectionManager) AddConnection(conn *websocket.Conn, key interface{}) {
	cm.addCh <- &AddConnectionRequest{Conn: conn, Key: key}
}

func (cm *ConnectionManager) RemoveConnection(key interface{}) {
	cm.removeCh <- key
}

func (cm *ConnectionManager) SendMessage(msg Message) {
	cm.sendCh <- msg
}

func Upgrade(w http.ResponseWriter, r *http.Request, key interface{}) error {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return err
	}

	cm.AddConnection(conn, key)

	for {
		if _, _, err := conn.NextReader(); err != nil {
			break
		}
	}

	cm.RemoveConnection(key)

	return nil
}

func makeWSPacket(data interface{}) []byte {
	b, err := json.Marshal(data)
	if err != nil {
		log.Warn("Marshalling websocket packet message.")
		b = marshalError()
	}

	return b
}

func marshalError() []byte {
	now := time.Now().Format(time.RFC3339Nano)
	return []byte(fmt.Sprintf(`{"type": "error", "message": null, "error": "marshal failed", "ts_sent": "%s"}`, now))
}

func sendWebsocketMessage(key interface{}, data interface{}, msgType string) {
	msg := Message{
		ConnectionID: key,
		Data:         makeWSPacket(WSPacket{Type: msgType, Message: data, TSSending: time.Now()}),
	}
	cm.SendMessage(msg)
}

func setMultipartFormDataFields(s interface{}, r *http.Request) error {
	// Get the value and type of the struct
	v := reflect.ValueOf(s).Elem()
	t := v.Type()

	// Loop through all the fields in the struct
	for i := 0; i < t.NumField(); i++ {
		// Get the field and tag for each field
		field := t.Field(i)
		tag := field.Tag
		tagValue := tag.Get("schema")

		// Get the value of the field
		fieldValue := v.FieldByName(field.Name)

		// If the field is valid and can be set, handle its value based on its type
		if fieldValue.IsValid() && fieldValue.CanSet() {
			switch {
			// If the type of the field is a File struct, retrieves the file from the request, sets the FilePath and ReadSeeker fields in a new File instance, and sets the field value to the new File instance.
			case field.Type == reflect.TypeOf(File{}):
				file, fileHeader, err := r.FormFile(tagValue)
				if err != nil {
					// If no file was found, create a new File instance with empty values
					if err == http.ErrMissingFile {
						fileValue := File{
							FilePath:   "",
							ReadSeeker: bytes.NewReader([]byte{}),
						}
						fieldValue.Set(reflect.ValueOf(fileValue))
						continue
					}
					return fmt.Errorf("failed to retrieve file '%s': %s", tagValue, err)
				}
				defer file.Close()

				fileValue := File{
					FilePath:   fileHeader.Filename,
					ReadSeeker: file,
				}
				fieldValue.Set(reflect.ValueOf(fileValue))

			default:
				// For all other field types, get the value from the request and set the field's value
				value := r.FormValue(tagValue)
				if value != "" {
					valueOf := reflect.ValueOf(value)

					if fieldValue.Kind() == reflect.Ptr {
						if fieldValue.IsNil() {
							fieldValue.Set(reflect.New(fieldValue.Type().Elem()))
						}
						fieldValue = fieldValue.Elem()
					}

					if valueOf.Type().ConvertibleTo(fieldValue.Type()) {
						fieldValue.Set(valueOf.Convert(fieldValue.Type()))
					} else {
						switch fieldType := fieldValue.Type().Name(); fieldType {
						case "int", "int32", "int64":
							intValue, err := strconv.Atoi(value)
							if err != nil {
								return fmt.Errorf("failed to convert field '%s' to int: %s", field.Name, err)
							}
							fieldValue.SetInt(int64(intValue))
						case "bool":
							boolValue, err := strconv.ParseBool(value)
							if err != nil {
								return fmt.Errorf("failed to convert field '%s' to bool: %s", field.Name, err)
							}
							fieldValue.SetBool(boolValue)
						case "Time":
							timeValue, err := time.Parse(time.RFC3339, value)
							if err != nil {
								return fmt.Errorf("failed to convert field '%s' to Time: %s", field.Name, err)
							}
							fieldValue.Set(reflect.ValueOf(timeValue))
						case "Date":
							dateValue, err := parseDate(value)
							if err != nil {
								return fmt.Errorf("failed to convert field '%s' to Date: %s", field.Name, err)
							}
							fieldValue.Set(reflect.ValueOf(dateValue))
						default:
							return fmt.Errorf("type mismatch for field '%s': expected %s, got %s", field.Name, fieldValue.Type(), valueOf.Type())
						}
					}
				}
			}
		}
	}

	return nil
}

// setSecretsAsEnv converts a JSON object into environment variables.
// Any non-string values (e.g., numbers, bools, nested objects)
// are safely converted to strings as well.
func setSecretsAsEnv(jsonStr string) error {
	var data map[string]string
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return fmt.Errorf("failed to unmarshal secrets JSON: %w", err)
	}

	for k, v := range data {
		envKey := strings.ToUpper(k)
		if err := os.Setenv(envKey, v); err != nil {
			return fmt.Errorf("failed to set env var %s: %w", envKey, err)
		}
	}
	return nil
}

// UpdateGlobalVariables refreshes the declared global variables with their current OS environment values.
func UpdateGlobalVariables() {
}
