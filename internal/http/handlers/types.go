package handlers

import "time"

// Order list response types

type PaymentSummary struct {
	ID            int64      `json:"id"`
	Status        string     `json:"status"`
	PaymentMethod string     `json:"paymentMethod"`
	PaidAt        *time.Time `json:"paidAt"`
}

type ReservationSummary struct {
	ID              int64   `json:"id"`
	PartySize       int32   `json:"partySize"`
	ReservationDate string  `json:"reservationDate"`
	ReservationTime string  `json:"reservationTime"`
	TableNumber     *string `json:"tableNumber"`
	Status          string  `json:"status"`
}

type CustomerSummary struct {
	ID    int64   `json:"id"`
	Name  string  `json:"name"`
	Phone *string `json:"phone"`
	Email *string `json:"email,omitempty"`
}

type DeliveryDriverSummary struct {
	ID    int64   `json:"id"`
	Name  string  `json:"name"`
	Email *string `json:"email"`
	Phone *string `json:"phone"`
}

type OrderCount struct {
	OrderItems int64 `json:"orderItems"`
}

type OrderListItem struct {
	ID                  int64                  `json:"id"`
	MerchantID          int64                  `json:"merchantId"`
	CustomerID          *int64                 `json:"customerId"`
	OrderNumber         string                 `json:"orderNumber"`
	OrderType           string                 `json:"orderType"`
	TableNumber         *string                `json:"tableNumber"`
	Status              string                 `json:"status"`
	IsScheduled         bool                   `json:"isScheduled"`
	ScheduledDate       *string                `json:"scheduledDate"`
	ScheduledTime       *string                `json:"scheduledTime"`
	DeliveryStatus      *string                `json:"deliveryStatus"`
	DeliveryUnit        *string                `json:"deliveryUnit"`
	DeliveryAddress     *string                `json:"deliveryAddress"`
	DeliveryFeeAmount   float64                `json:"deliveryFeeAmount"`
	DeliveryDistanceKm  *float64               `json:"deliveryDistanceKm"`
	DeliveryDeliveredAt *time.Time             `json:"deliveryDeliveredAt"`
	Subtotal            float64                `json:"subtotal"`
	TaxAmount           float64                `json:"taxAmount"`
	ServiceChargeAmount float64                `json:"serviceChargeAmount"`
	PackagingFeeAmount  float64                `json:"packagingFeeAmount"`
	DiscountAmount      float64                `json:"discountAmount"`
	TotalAmount         float64                `json:"totalAmount"`
	Notes               *string                `json:"notes"`
	AdminNote           *string                `json:"adminNote"`
	KitchenNotes        *string                `json:"kitchenNotes"`
	CreatedAt           time.Time              `json:"createdAt"`
	PlacedAt            time.Time              `json:"placedAt"`
	UpdatedAt           time.Time              `json:"updatedAt"`
	EditedAt            *time.Time             `json:"editedAt"`
	EditedByUserID      *int64                 `json:"editedByUserId"`
	Payment             *PaymentSummary        `json:"payment,omitempty"`
	Reservation         *ReservationSummary    `json:"reservation,omitempty"`
	Customer            *CustomerSummary       `json:"customer,omitempty"`
	DeliveryDriver      *DeliveryDriverSummary `json:"deliveryDriver,omitempty"`
	Count               *OrderCount            `json:"_count,omitempty"`
}

type OrderItemAddon struct {
	ID       int64   `json:"id"`
	Name     string  `json:"name"`
	Price    float64 `json:"price"`
	Quantity int32   `json:"quantity"`
}

type OrderItem struct {
	ID        int64            `json:"id"`
	MenuName  string           `json:"menuName"`
	Quantity  int32            `json:"quantity"`
	MenuPrice float64          `json:"menuPrice"`
	Subtotal  float64          `json:"subtotal"`
	Notes     *string          `json:"notes"`
	Addons    []OrderItemAddon `json:"addons"`
}

type OrderDetail struct {
	ID                  int64       `json:"id"`
	OrderNumber         string      `json:"orderNumber"`
	Status              string      `json:"status"`
	OrderType           string      `json:"orderType"`
	TableNumber         *string     `json:"tableNumber"`
	CustomerName        string      `json:"customerName"`
	Subtotal            float64     `json:"subtotal"`
	TaxAmount           float64     `json:"taxAmount"`
	ServiceChargeAmount float64     `json:"serviceChargeAmount"`
	PackagingFeeAmount  float64     `json:"packagingFeeAmount"`
	DiscountAmount      float64     `json:"discountAmount"`
	TotalAmount         float64     `json:"totalAmount"`
	CreatedAt           time.Time   `json:"createdAt"`
	UpdatedAt           time.Time   `json:"updatedAt"`
	PlacedAt            *time.Time  `json:"placedAt"`
	CompletedAt         *time.Time  `json:"completedAt"`
	DeliveryStatus      *string     `json:"deliveryStatus"`
	DeliveryUnit        *string     `json:"deliveryUnit"`
	DeliveryAddress     *string     `json:"deliveryAddress"`
	DeliveryFeeAmount   float64     `json:"deliveryFeeAmount"`
	DeliveryDistanceKm  *float64    `json:"deliveryDistanceKm"`
	DeliveryDeliveredAt *time.Time  `json:"deliveryDeliveredAt"`
	EditedAt            *time.Time  `json:"editedAt"`
	ChangedByAdmin      bool        `json:"changedByAdmin"`
	OrderItems          []OrderItem `json:"orderItems"`
	Merchant            struct {
		Name     string `json:"name"`
		Currency string `json:"currency"`
		Code     string `json:"code"`
	} `json:"merchant"`
	Payment *struct {
		Status                  *string        `json:"status"`
		PaymentMethod           *string        `json:"paymentMethod"`
		Amount                  *float64       `json:"amount"`
		PaidAt                  *time.Time     `json:"paidAt"`
		CustomerPaidAt          *time.Time     `json:"customerPaidAt"`
		CustomerProofUrl        *string        `json:"customerProofUrl"`
		CustomerProofUploadedAt *time.Time     `json:"customerProofUploadedAt"`
		CustomerPaymentNote     *string        `json:"customerPaymentNote"`
		CustomerProofMeta       map[string]any `json:"customerProofMeta"`
	} `json:"payment"`
	Reservation *struct {
		Status          string  `json:"status"`
		PartySize       int32   `json:"partySize"`
		ReservationDate string  `json:"reservationDate"`
		ReservationTime string  `json:"reservationTime"`
		TableNumber     *string `json:"tableNumber"`
	} `json:"reservation"`
}
