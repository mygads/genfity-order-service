package handlers

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"genfity-order-services/internal/auth"
	"genfity-order-services/internal/middleware"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"golang.org/x/crypto/bcrypt"
)

type staffCreateRequest struct {
	Name     string `json:"name"`
	Email    string `json:"email"`
	Password string `json:"password"`
	Phone    string `json:"phone"`
}

type staffUpdateRequest struct {
	Name        *string `json:"name"`
	Phone       *string `json:"phone"`
	NewPassword *string `json:"newPassword"`
}

type staffPermissionsRequest struct {
	Permissions []string `json:"permissions"`
}

type staffToggleRequest struct {
	IsActive *bool `json:"isActive"`
}

func (h *Handler) MerchantStaffList(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}
	if !authCtx.IsOwner {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "Owner access required")
		return
	}

	search := strings.TrimSpace(r.URL.Query().Get("search"))

	rows, err := h.DB.Query(ctx, `
        select mu.id, mu.user_id, u.name, u.email, u.phone, u.role, mu.is_active, mu.created_at,
               mu.permissions, mu.invitation_status, mu.invited_at, mu.accepted_at
        from merchant_users mu
        join users u on u.id = mu.user_id
        where mu.merchant_id = $1
          and ($2 = '' or u.name ilike '%' || $2 || '%' or u.email ilike '%' || $2 || '%')
        order by case when u.role = 'MERCHANT_OWNER' then 0 else 1 end, mu.created_at desc
    `, *authCtx.MerchantID, search)
	if err != nil {
		h.Logger.Error("merchant staff list query failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve staff")
		return
	}
	defer rows.Close()

	staff := make([]map[string]any, 0)
	for rows.Next() {
		var (
			id               int64
			userID           int64
			name             string
			email            string
			phone            pgtype.Text
			role             string
			isActive         bool
			createdAt        time.Time
			permissions      []string
			invitationStatus string
			invitedAt        pgtype.Timestamptz
			acceptedAt       pgtype.Timestamptz
		)
		if err := rows.Scan(&id, &userID, &name, &email, &phone, &role, &isActive, &createdAt, &permissions, &invitationStatus, &invitedAt, &acceptedAt); err != nil {
			h.Logger.Error("merchant staff list scan failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve staff")
			return
		}

		staff = append(staff, map[string]any{
			"id":               id,
			"userId":           userID,
			"name":             name,
			"email":            email,
			"phone":            nullableText(phone),
			"role":             role,
			"isActive":         isActive,
			"joinedAt":         createdAt,
			"permissions":      permissions,
			"invitationStatus": invitationStatus,
			"invitedAt":        nullableTime(invitedAt),
			"acceptedAt":       nullableTime(acceptedAt),
		})
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"data":       map[string]any{"staff": staff},
		"message":    "Staff retrieved successfully",
		"statusCode": 200,
	})
}

func (h *Handler) MerchantStaffCreate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}
	if !authCtx.IsOwner {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "Owner access required")
		return
	}

	var body staffCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	name := strings.TrimSpace(body.Name)
	email := strings.ToLower(strings.TrimSpace(body.Email))
	password := strings.TrimSpace(body.Password)
	phone := strings.TrimSpace(body.Phone)

	if name == "" || email == "" || password == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Name, email, and password are required")
		return
	}
	if !strings.Contains(email, "@") {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid email")
		return
	}

	var existingID int64
	if err := h.DB.QueryRow(ctx, "select id from users where lower(email) = lower($1)", email).Scan(&existingID); err == nil {
		response.Error(w, http.StatusConflict, "EMAIL_ALREADY_EXISTS", "Email already registered")
		return
	} else if err != pgx.ErrNoRows {
		h.Logger.Error("staff email check failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create staff")
		return
	}

	var merchantName string
	if err := h.DB.QueryRow(ctx, "select name from merchants where id = $1", *authCtx.MerchantID).Scan(&merchantName); err != nil {
		if err == pgx.ErrNoRows {
			response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found")
			return
		}
		h.Logger.Error("merchant lookup failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create staff")
		return
	}

	hashed, err := bcrypt.GenerateFromPassword([]byte(password), 10)
	if err != nil {
		h.Logger.Error("staff password hash failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create staff")
		return
	}

	var (
		userID     int64
		userName   string
		userEmail  string
		userPhone  pgtype.Text
		userRole   string
		userActive bool
	)
	if err := h.DB.QueryRow(ctx, `
        insert into users (name, email, phone, password_hash, role, is_active, must_change_password)
        values ($1, $2, $3, $4, 'MERCHANT_STAFF', true, false)
        returning id, name, email, phone, role, is_active
    `, name, email, nullableString(phone), string(hashed)).Scan(&userID, &userName, &userEmail, &userPhone, &userRole, &userActive); err != nil {
		h.Logger.Error("staff user create failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create staff")
		return
	}

	_, err = h.DB.Exec(ctx, `
        insert into merchant_users (merchant_id, user_id, role, is_active, invitation_status, accepted_at)
        values ($1, $2, 'STAFF', true, 'ACCEPTED', now())
    `, *authCtx.MerchantID, userID)
	if err != nil {
		h.Logger.Error("staff link create failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create staff")
		return
	}

	_ = merchantName
	response.JSON(w, http.StatusCreated, map[string]any{
		"success": true,
		"data": map[string]any{
			"staff": map[string]any{
				"id":       userID,
				"name":     userName,
				"email":    userEmail,
				"phone":    nullableText(userPhone),
				"role":     userRole,
				"isActive": userActive,
			},
		},
		"message":    "Staff created successfully",
		"statusCode": 201,
	})
}

func (h *Handler) MerchantStaffDelete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}
	if !authCtx.IsOwner {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "Owner access required")
		return
	}

	userID := int64(0)
	if pathValue := readPathString(r, "id"); pathValue != "" {
		parsed, err := parseInt64Value(pathValue)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid userId")
			return
		}
		userID = parsed
	} else {
		parsed, err := readQueryInt64(r, "userId")
		if err != nil {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "User ID required")
			return
		}
		userID = parsed
	}
	if userID == authCtx.UserID {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Cannot remove yourself")
		return
	}

	var (
		merchantUserID int64
		userRole       string
	)
	if err := h.DB.QueryRow(ctx, `
        select mu.id, u.role
        from merchant_users mu
        join users u on u.id = mu.user_id
        where mu.merchant_id = $1 and mu.user_id = $2
    `, *authCtx.MerchantID, userID).Scan(&merchantUserID, &userRole); err != nil {
		if err == pgx.ErrNoRows {
			response.Error(w, http.StatusNotFound, "NOT_FOUND", "Staff member not found")
			return
		}
		h.Logger.Error("staff lookup failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to remove staff")
		return
	}

	if userRole == string(auth.RoleMerchantOwner) {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Cannot remove another owner")
		return
	}

	if _, err := h.DB.Exec(ctx, "delete from merchant_users where id = $1", merchantUserID); err != nil {
		h.Logger.Error("staff delete failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to remove staff")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"data":       nil,
		"message":    "Staff removed successfully",
		"statusCode": 200,
	})
}

func (h *Handler) MerchantStaffInvite(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}
	if !authCtx.IsOwner {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "Owner access required")
		return
	}

	var body struct {
		Email string `json:"email"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	email := strings.ToLower(strings.TrimSpace(body.Email))
	if email == "" || !strings.Contains(email, "@") {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Valid email is required")
		return
	}

	var merchantName string
	if err := h.DB.QueryRow(ctx, "select name from merchants where id = $1", *authCtx.MerchantID).Scan(&merchantName); err != nil {
		if err == pgx.ErrNoRows {
			response.Error(w, http.StatusNotFound, "NOT_FOUND", "Merchant not found")
			return
		}
		h.Logger.Error("merchant lookup failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to invite staff")
		return
	}

	var (
		userID int64
		name   string
		role   string
	)
	if err := h.DB.QueryRow(ctx, "select id, name, role from users where lower(email) = lower($1)", email).Scan(&userID, &name, &role); err != nil {
		if err == pgx.ErrNoRows {
			response.Error(w, http.StatusNotFound, "NOT_FOUND", "Email not registered. User must register first before being invited.")
			return
		}
		h.Logger.Error("staff invite lookup failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to invite staff")
		return
	}

	if role == string(auth.RoleSuperAdmin) {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Cannot add this user as staff")
		return
	}

	var existingLink int64
	if err := h.DB.QueryRow(ctx, "select id from merchant_users where merchant_id = $1 and user_id = $2", *authCtx.MerchantID, userID).Scan(&existingLink); err == nil {
		response.Error(w, http.StatusConflict, "EMAIL_ALREADY_EXISTS", "User is already a staff member")
		return
	} else if err != pgx.ErrNoRows {
		h.Logger.Error("staff invite link check failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to invite staff")
		return
	}

	if role == string(auth.RoleMerchantStaff) {
		var otherCount int64
		if err := h.DB.QueryRow(ctx, `
            select count(1)
            from merchant_users mu
            join merchants m on m.id = mu.merchant_id
            where mu.user_id = $1
              and mu.is_active = true
              and mu.role in ('OWNER', 'STAFF')
              and m.is_active = true
              and mu.merchant_id <> $2
        `, userID, *authCtx.MerchantID).Scan(&otherCount); err != nil {
			h.Logger.Error("staff invite merchant check failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to invite staff")
			return
		}
		if otherCount > 0 {
			response.Error(w, http.StatusForbidden, "FORBIDDEN", "This staff account is already linked to another merchant. Staff accounts must have exactly one merchant.")
			return
		}
	}

	inviteToken, err := generateInviteToken(32)
	if err != nil {
		h.Logger.Error("staff invite token failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to invite staff")
		return
	}

	_, err = h.DB.Exec(ctx, `
        insert into merchant_users (merchant_id, user_id, role, is_active, invitation_status, invited_at, invite_token, invite_token_expires_at)
        values ($1, $2, 'STAFF', true, 'WAITING', now(), $3, $4)
    `, *authCtx.MerchantID, userID, inviteToken, time.Now().UTC().Add(7*24*time.Hour))
	if err != nil {
		h.Logger.Error("staff invite create failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to invite staff")
		return
	}

	_ = merchantName
	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"userId": fmt.Sprint(userID),
			"email":  email,
			"name":   name,
		},
		"message":    "Staff invitation sent successfully",
		"statusCode": 200,
	})
}

func (h *Handler) MerchantStaffLeave(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}
	if authCtx.Role != auth.RoleMerchantStaff {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Only staff can leave a merchant. Owners must transfer ownership first.")
		return
	}

	var body struct {
		MerchantID string `json:"merchantId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	merchantID, err := parseInt64Value(body.MerchantID)
	if err != nil || merchantID == 0 {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Merchant ID required")
		return
	}
	if merchantID != *authCtx.MerchantID {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "Merchant ID mismatch")
		return
	}

	if _, err := h.DB.Exec(ctx, "delete from merchant_users where merchant_id = $1 and user_id = $2", merchantID, authCtx.UserID); err != nil {
		h.Logger.Error("staff leave failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to leave merchant")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"data":       nil,
		"message":    "You have successfully left the merchant",
		"statusCode": 200,
	})
}

func (h *Handler) MerchantStaffUpdate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}
	if !authCtx.IsOwner {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "Owner access required")
		return
	}

	userID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid staff id")
		return
	}

	var body staffUpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	if body.Name == nil && body.Phone == nil && body.NewPassword == nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "No changes provided")
		return
	}

	var invitationStatus string
	var staffRole string
	if err := h.DB.QueryRow(ctx, `
        select invitation_status, role
        from merchant_users
        where merchant_id = $1 and user_id = $2
    `, *authCtx.MerchantID, userID).Scan(&invitationStatus, &staffRole); err != nil {
		if err == pgx.ErrNoRows {
			response.Error(w, http.StatusNotFound, "NOT_FOUND", "Staff not found")
			return
		}
		h.Logger.Error("staff lookup failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update staff")
		return
	}

	if staffRole != "STAFF" {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Staff not found")
		return
	}

	if invitationStatus != "ACCEPTED" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Staff invitation must be accepted before editing")
		return
	}

	updates := make([]string, 0)
	args := make([]any, 0)
	argPos := 1

	if body.Name != nil {
		name := strings.TrimSpace(*body.Name)
		updates = append(updates, fmt.Sprintf("name = $%d", argPos))
		args = append(args, name)
		argPos++
	}
	if body.Phone != nil {
		phone := strings.TrimSpace(*body.Phone)
		updates = append(updates, fmt.Sprintf("phone = $%d", argPos))
		args = append(args, nullableString(phone))
		argPos++
	}
	if body.NewPassword != nil && strings.TrimSpace(*body.NewPassword) != "" {
		hashed, err := bcrypt.GenerateFromPassword([]byte(strings.TrimSpace(*body.NewPassword)), 10)
		if err != nil {
			h.Logger.Error("staff password hash failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update staff")
			return
		}
		updates = append(updates, fmt.Sprintf("password_hash = $%d", argPos))
		args = append(args, string(hashed))
		argPos++
		updates = append(updates, "must_change_password = false")
	}

	if len(updates) == 0 {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "No changes provided")
		return
	}

	query := fmt.Sprintf("update users set %s where id = $%d returning id, name, email, phone, role, is_active", strings.Join(updates, ", "), argPos)
	args = append(args, userID)

	var (
		updatedID     int64
		updatedName   string
		updatedEmail  string
		updatedPhone  pgtype.Text
		updatedRole   string
		updatedActive bool
	)
	if err := h.DB.QueryRow(ctx, query, args...).Scan(&updatedID, &updatedName, &updatedEmail, &updatedPhone, &updatedRole, &updatedActive); err != nil {
		h.Logger.Error("staff update failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update staff")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"staff": map[string]any{
				"id":       updatedID,
				"name":     updatedName,
				"email":    updatedEmail,
				"phone":    nullableText(updatedPhone),
				"role":     updatedRole,
				"isActive": updatedActive,
			},
		},
		"message":    "Staff updated successfully",
		"statusCode": 200,
	})
}

func (h *Handler) MerchantStaffPermissionsGet(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}
	if !authCtx.IsOwner {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "Owner access required")
		return
	}

	userID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Staff ID required")
		return
	}

	var (
		role        string
		permissions []string
		isActive    bool
	)
	if err := h.DB.QueryRow(ctx, `
        select role, permissions, is_active
        from merchant_users
        where merchant_id = $1 and user_id = $2
    `, *authCtx.MerchantID, userID).Scan(&role, &permissions, &isActive); err != nil {
		if err == pgx.ErrNoRows {
			response.Error(w, http.StatusNotFound, "NOT_FOUND", "Staff member not found")
			return
		}
		h.Logger.Error("staff permissions lookup failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve staff permissions")
		return
	}

	isOwner := role == "OWNER"
	if isOwner {
		permissions = auth.AllStaffPermissions()
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"userId":      fmt.Sprint(userID),
			"permissions": permissions,
			"role":        role,
			"isOwner":     isOwner,
			"isActive":    isActive,
		},
		"message":    "Staff permissions retrieved successfully",
		"statusCode": 200,
	})
}

func (h *Handler) MerchantStaffPermissionsUpdate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}
	if !authCtx.IsOwner {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "Owner access required")
		return
	}

	userID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Staff ID required")
		return
	}

	var body staffPermissionsRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}
	if body.Permissions == nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Permissions must be an array")
		return
	}

	var role string
	if err := h.DB.QueryRow(ctx, `
        select role
        from merchant_users
        where merchant_id = $1 and user_id = $2
    `, *authCtx.MerchantID, userID).Scan(&role); err != nil {
		if err == pgx.ErrNoRows {
			response.Error(w, http.StatusNotFound, "NOT_FOUND", "Staff member not found")
			return
		}
		h.Logger.Error("staff permissions lookup failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update permissions")
		return
	}

	if role == "OWNER" {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "Cannot modify owner permissions")
		return
	}

	allowed := auth.AllStaffPermissions()
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, value := range allowed {
		allowedSet[value] = struct{}{}
	}
	filtered := make([]string, 0)
	for _, perm := range body.Permissions {
		if _, ok := allowedSet[perm]; ok {
			filtered = append(filtered, perm)
		}
	}

	if _, err := h.DB.Exec(ctx, `
        update merchant_users
        set permissions = $1, updated_at = now()
        where merchant_id = $2 and user_id = $3
    `, filtered, *authCtx.MerchantID, userID); err != nil {
		h.Logger.Error("staff permissions update failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update permissions")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"userId":      fmt.Sprint(userID),
			"permissions": filtered,
		},
		"message":    "Staff permissions updated successfully",
		"statusCode": 200,
	})
}

func (h *Handler) MerchantStaffPermissionsToggle(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}
	if !authCtx.IsOwner {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "Owner access required")
		return
	}

	userID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Staff ID required")
		return
	}

	var body staffToggleRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}
	if body.IsActive == nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "isActive must be a boolean")
		return
	}

	var role string
	if err := h.DB.QueryRow(ctx, `
        select role
        from merchant_users
        where merchant_id = $1 and user_id = $2
    `, *authCtx.MerchantID, userID).Scan(&role); err != nil {
		if err == pgx.ErrNoRows {
			response.Error(w, http.StatusNotFound, "NOT_FOUND", "Staff member not found")
			return
		}
		h.Logger.Error("staff toggle lookup failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update staff status")
		return
	}

	if role == "OWNER" {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "Cannot modify owner status")
		return
	}

	if _, err := h.DB.Exec(ctx, `
        update merchant_users
        set is_active = $1, updated_at = now()
        where merchant_id = $2 and user_id = $3
    `, *body.IsActive, *authCtx.MerchantID, userID); err != nil {
		h.Logger.Error("staff toggle update failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update staff status")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"userId":   fmt.Sprint(userID),
			"isActive": *body.IsActive,
		},
		"message":    fmt.Sprintf("Staff %s successfully", map[bool]string{true: "activated", false: "deactivated"}[*body.IsActive]),
		"statusCode": 200,
	})
}

func nullableText(value pgtype.Text) any {
	if value.Valid {
		return value.String
	}
	return nil
}

func readQueryInt64(r *http.Request, key string) (int64, error) {
	value := strings.TrimSpace(r.URL.Query().Get(key))
	if value == "" {
		return 0, fmt.Errorf("missing param")
	}
	return parseInt64Value(value)
}

func generateInviteToken(size int) (string, error) {
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func (h *Handler) ensureMerchantUser(ctx context.Context, merchantID int64, userID int64) (bool, error) {
	var exists bool
	if err := h.DB.QueryRow(ctx, `select exists(select 1 from merchant_users where merchant_id = $1 and user_id = $2)`, merchantID, userID).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}
