package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

func parseVerifyURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || len(raw) > 2048 || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.Fragment != "" {
		return nil, errors.New("invalid auth_verify_url")
	}
	return u, nil
}

// verifyAuthorization never sends the node's bearer token to the verifier.
func (s *server) verifyAuthorization(ctx context.Context, rawURL, action string) error {
	if rawURL == "" {
		return nil
	}
	u, err := parseVerifyURL(rawURL)
	if err != nil {
		return err
	}
	if action != "" {
		query := u.Query()
		query.Set("action", action)
		u.RawQuery = query.Encode()
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}
	client := *s.httpClient
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 17))
	if err != nil || resp.StatusCode != http.StatusOK || len(body) > 16 || !strings.EqualFold(strings.TrimSpace(string(body)), "ok") {
		return errors.New("authorization denied")
	}
	return nil
}
