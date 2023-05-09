package types

import (
	"errors"
	"fmt"
	"github.com/twmb/murmur3"
	"regexp"
	"strconv"
)

var (
	ErrInvalidArgument       = errors.New("invalid argument")
	ErrInvalidTopic          = fmt.Errorf("invalid topic: %w", ErrInvalidArgument)
	ErrInvalidSubscriptionId = fmt.Errorf("invalid subscription id: %w", ErrInvalidArgument)
)

type Topic string

var topicRegex = regexp.MustCompile(`^([a-zA-Z0-9_]{1,128})$`)

func (t Topic) Validate() error {
	if !topicRegex.MatchString(string(t)) {
		return ErrInvalidTopic
	}
	return nil
}

type SubscriptionId string

var subscriptionIdRegex = regexp.MustCompile(`^([a-zA-Z0-9_]{1,128})$`)

func (s SubscriptionId) Validate() error {
	if !subscriptionIdRegex.MatchString(string(s)) {
		return ErrInvalidSubscriptionId
	}
	return nil
}

type Msg struct {
	Key  Key
	Data []byte
}

const InitialHashRangeSize = HashRangeSize(15)

const MaxHash = Hash(int(1)<<16 - 1)

type Hash uint16

func (h Hash) HashRange(hrs HashRangeSize) HashRange {
	from := h - Hash(int(h)%hrs.Size())

	return HashRange{
		From: from,
		To:   from + Hash(hrs.Size()-1),
	}
}

type Key string

const MaxKeyLength = 128

func NewKey(str string) (Key, error) {
	if len(str) > 128 {
		return "", errors.New("key too long")
	}
	return Key(str), nil
}

func (k Key) Hash() Hash {
	return Hash(murmur3.StringSum64(string(k)) % uint64(MaxHash))
}

type HashRangeSize uint8 // exponent for power of 2 between 0 and 16

func (hrs HashRangeSize) Size() int {
	return 1 << int(hrs)
}

var ErrInvalidHashRangeSize = errors.New("invalid hash range size")

func ParseHashRangeSize(str string) (HashRangeSize, error) {
	n, err := strconv.Atoi(str)
	if err != nil {
		return 0, ErrInvalidHashRangeSize
	}

	if n < 0 || n > 16 {
		return 0, ErrInvalidHashRangeSize
	}

	return HashRangeSize(n), nil
}

func (hrs HashRangeSize) HashRanges() []HashRange {
	var ranges []HashRange
	for i := 0; i < int(MaxHash); i += hrs.Size() {

		ranges = append(ranges, HashRange{
			From: Hash(i),
			To:   Hash(i + hrs.Size() - 1), // inclusive, therefore minus 1
		})
	}
	return ranges
}

func (hrs HashRangeSize) ValidHashRange(hashRange HashRange) bool {
	if int(hashRange.To-hashRange.From+1) != hrs.Size() {
		return false
	}

	if int(hashRange.From)%hrs.Size() != 0 {
		return false
	}

	return true
}

type HashRange struct {
	From Hash // inclusive
	To   Hash // inclusive
}

func ParseHashRange(str string) (HashRange, error) {
	var hr HashRange
	_, err := fmt.Sscanf(str, "%d-%d", &hr.From, &hr.To)
	return hr, err
}

func (r *HashRange) Equal(hr HashRange) bool {
	return r.From == hr.From && r.To == hr.To
}

func (r *HashRange) Contains(h Hash) bool {
	return r.From <= h && h <= r.To
}

func (r *HashRange) Overlaps(hr HashRange) bool {
	return r.From <= hr.To && hr.From <= r.To
}

func (r *HashRange) Size() uint16 {
	return uint16(r.To - r.From + 1)
}

func (r *HashRange) String() string {
	return strconv.Itoa(int(r.From)) + "-" + strconv.Itoa(int(r.To))
}
